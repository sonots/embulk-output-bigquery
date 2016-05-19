require 'google/apis/storage_v1'
require 'google/api_client/auth/key_utils'
require 'zlib'
require 'json'
require 'csv'
require_relative 'abstract_writer'
require_relative 'value_converter_factory'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class GcsWriter < AbstractWriter
        def initialize(task, schema, index, converters = nil)
          super(task, schema, index, converters)

          @auth_method = task['auth_method']
          @private_key_path = task['p12_keyfile']
          @private_key_passphrase = 'notasecret'
          @json_key = task['json_keyfile']

          @project = task['project']
          @bucket = task['gcs_bucket']

          insert_or_patch_lifecycle_bucket
          purge_objects(task['path_prefix'])
        end

        def lazy_initialize
          return nil if @lazy_initialized
          @z = Zlib::Deflate.new
          @io_r, @io_w = IO.pipe
          @path = sprintf(
            "#{@task['path_prefix']}#{@task['sequence_format']}#{@task['file_ext']}",
            Process.pid, Thread.current.object_id
          )
          insert_object(@path, @io_r)
          @lazy_initialized = true
        end

        def close
          @io_w << @z.finish
          @io_w.close rescue nil
        end

        def add(page)
          lazy_initialize unless @lazy_initialized
          page.each do |record|
            Embulk.logger.trace { "embulk-output-bigquery: record #{record}" }
            formatted_record = @formatter_proc.call(record)
            Embulk.logger.trace { "embulk-output-bigquery: formatted_record #{formatted_record.chomp}" }
            @io_w << @z.deflate(formatted_record)
            @num_rows += 1
          end
          log_num_rows
          @num_rows
        end

        def client
          return @cached_client if @cached_client && @cached_client_expiration > Time.now

          client = Google::Apis::StorageV1::StorageService.new
          client.client_options.application_name = @task['application_name']
          client.request_options.retries = @task['retries']
          client.request_options.timeout_sec = @task['timeout_sec']
          client.request_options.open_timeout_sec = @task['open_timeout_sec']
          Embulk.logger.debug { "embulk-output-bigquery: gcs client_options: #{client.client_options.to_h}" }
          Embulk.logger.debug { "embulk-output-bigquery: gcs request_options: #{client.request_options.to_h}" }

          scope = "https://www.googleapis.com/auth/cloud-platform"

          case @auth_method
          when 'private_key'
            key = Google::APIClient::KeyUtils.load_from_pkcs12(@private_key_path, @private_key_passphrase)
            auth = Signet::OAuth2::Client.new(
              token_credential_uri: "https://accounts.google.com/o/oauth2/token",
              audience: "https://accounts.google.com/o/oauth2/token",
              scope: scope,
              issuer: @email,
              signing_key: key)

          when 'compute_engine'
            auth = Google::Auth::GCECredentials.new

          when 'json_key'
            if File.exist?(@json_key)
              auth = File.open(@json_key) do |f|
                Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: f, scope: scope)
              end
            else
              key = StringIO.new(@json_key)
              auth = Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: key, scope: scope)
            end

          when 'application_default'
            auth = Google::Auth.get_application_default([scope])

          else
            raise ConfigError, "Unknown auth method: #{@auth_method}"
          end

          client.authorization = auth

          @cached_client_expiration = Time.now + 1800
          @cached_client = client
        end

        def insert_bucket(bucket = nil)
          bucket ||= @bucket
          begin
            Embulk.logger.info { "embulk-output-bigquery: Create bucket... #{@project}:#{bucket}" }
            body  = {
              name: bucket,
            }
            opts = {}

            Embulk.logger.debug { "embulk-output-bigquery, insert_bucket(#{@project}, #{body}, #{opts})" }
            client.insert_bucket(@project, body, opts)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 409 && /conflict:/ =~ e.message
              # ignore 'Already Exists' error
              return nil
            end
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_bucket(#{@project}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to create bucket #{@project}:#{bucket}, response:#{response}"
          end
        end

        def get_bucket(bucket = nil)
          bucket ||= @bucket
          begin
            opts = {}
            Embulk.logger.debug { "embulk-output-bigquery, get_bucket(#{bucket}, #{opts})" }
            client.get_bucket(bucket, opts)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            if e.status_code == 404 && /^notFound:/ =~ e.message
              return nil
            end
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: get_bucket(#{bucket}, #{opts}), response:#{response}"
            }
            raise Error, "failed to get bucket #{@project}:#{bucket}, response:#{response}"
          end
        end

        def patch_bucket(bucket: nil, body: {})
          bucket ||= @bucket
          begin
            opts = {}
            Embulk.logger.debug { "embulk-output-bigquery, patch_bucket(#{bucket}, #{body}, #{opts})" }
            client.patch_bucket(bucket, body, opts)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: patch_bucket(#{bucket}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to patch bucket #{@project}:#{bucket}, response:#{response}"
          end
        end

        def insert_or_patch_lifecycle_bucket(bucket = nil)
          bucket ||= @bucket
          insert_response = insert_bucket(bucket)
          get_response = get_bucket(bucket)
          # apply lifecycle with patch method so that lifecycle is applied even if a bucket already exists
          if get_response.nil? || (get_response.lifecycle.nil? && @task['gcs_bucket_lifecycle_ttl_days'])
            patch_body = {
              lifecycle: {
                rule:
                [
                  {
                    action: {type: "Delete"},
                    condition: {age: @task['gcs_bucket_lifecycle_ttl_days']}
                  }
                ]
              }
            }
            patch_response = patch_bucket(bucket, body: patch_body)
            get_response = get_bucket(bucket)
          end
          get_response
        end

        def insert_object(path, io, bucket: nil)
          bucket ||= @bucket
          gcs_path = "gs://#{File.join(bucket, path)}"
          begin
            Embulk.logger.info { "embulk-output-bigquery: Create object... #{@project}:#{gcs_path}" }
            body = {
              name: path,
            }
            opts = {
              upload_source: io,
              content_type: 'application/json'
            }

            Embulk.logger.debug { "embulk-output-bigquery, insert_object(#{bucket}, #{body}, #{opts})" }
            client.insert_object(bucket, body, opts)
          rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_object(#{bucket}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to create object #{@project}:#{gcs_path}, response:#{response}"
          end
        end

        def purge_objects(path_prefix, bucket: nil)
          bucket ||= @bucket
        end
      end
    end
  end
end
