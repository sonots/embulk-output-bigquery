require 'zlib'
require 'json'
require 'csv'
require_relative 'value_converter_factory'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class GcsWriter
        attr_reader :num_rows

        def initialize(task, schema, index, converters = nil)
          @task = task
          @schema = schema
          @index = index
          @converters = converters || ValueConverterFactory.create_converters(task, schema)

          @num_rows = 0
          @progress_log_timer = Time.now
          @previous_num_rows = 0

          if @task['payload_column_index']
            @payload_column_index = @task['payload_column_index']
            @formatter_proc = self.method(:to_payload)
          else
            case @task['source_format'].downcase
            when 'csv'
              @formatter_proc = self.method(:to_csv)
            else
              @formatter_proc = self.method(:to_jsonl)
            end
          end

          @auth_method = task['auth_method']
          @private_key_path = task['p12_keyfile']
          @private_key_passphrase = 'notasecret'
          @json_key = task['json_keyfile']

          @project = task['project']
          @bucket = task['bucket']
        end

        def client
          return @cached_client if @cached_client && @cached_client_expiration > Time.now

          client = Google::Apis::StorageV1::StorageService.new
          client.client_options.application_name = @task['application_name']
          client.request_options.retries = @task['retries']
          client.request_options.timeout_sec = @task['timeout_sec']
          client.request_options.open_timeout_sec = @task['open_timeout_sec']
          logger.debug { "embulk-output-bigquery: gcs client_options: #{client.client_options.to_h}" }
          logger.debug { "embulk-output-bigquery: gcs request_options: #{client.request_options.to_h}" }

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
            if e.status_code == 409 && /Already Exists:/ =~ e.message
              # ignore 'Already Exists' error
              return nil
            end
            response = {status_code: e.status_code, message: e.message, error_class: e.class}
            Embulk.logger.error {
              "embulk-output-bigquery: insert_bucket(#{@project}, #{body}, #{opts}), response:#{response}"
            }
            raise Error, "failed to create dataset #{@project}:#{dataset}, response:#{response}"
          end
        end

        def get_bucket(bucket: nil)
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
          if get_response.nil? || (get_response.lifecycle.nil? && @task['bucket_lifecycle_ttl_days'])
            patch_body = {
              lifecycle: {
                rule:
                [
                  {
                    action: {type: "Delete"},
                    condition: {age: @task['bucket_lifecycle_ttl_days']}
                  }
                ]
              }
            }
            patch_response = patch_bucket(bucket, body: patch_body)
            get_response = get_bucket(bucket)
          end
          get_response
        end

        def io
          return @io if @io

          path = sprintf(
            "#{@task['path_prefix']}#{@task['sequence_format']}#{@task['file_ext']}",
            Process.pid, Thread.current.object_id
          )
          if File.exist?(path)
            Embulk.logger.warn { "embulk-output-bigquery: unlink already existing #{path}" }
            File.unlink(path) rescue nil
          end
          Embulk.logger.info { "embulk-output-bigquery: create #{path}" }

          @io = open(path, 'w')
        end

        def open(path, mode = 'w')
          file_io = File.open(path, mode)
          case @task['compression'].downcase
          when 'gzip'
            io = Zlib::GzipWriter.new(file_io)
          else
            io = file_io
          end
          io
        end

        def close
          io.close rescue nil
          io
        end

        def reopen
          @io = open(io.path, 'a')
        end

        def to_payload(record)
          "#{record[@payload_column_index]}\n"
        end

        def to_csv(record)
          record.map.with_index do |value, column_index|
            @converters[column_index].call(value)
          end.to_csv
        end

        def to_jsonl(record)
          hash = {}
          column_names = @schema.names
          record.each_with_index do |value, column_index|
            column_name = column_names[column_index]
            hash[column_name] = @converters[column_index].call(value)
          end
          "#{hash.to_json}\n"
        end

        def num_format(number)
          number.to_s.gsub(/(\d)(?=(\d{3})+(?!\d))/, '\1,')
        end

        def add(page)
          _io = io
          # I once tried to split IO writing into another IO thread using SizedQueue
          # However, it resulted in worse performance, so I removed the codes.
          page.each do |record|
            Embulk.logger.trace { "embulk-output-bigquery: record #{record}" }
            formatted_record = @formatter_proc.call(record)
            Embulk.logger.trace { "embulk-output-bigquery: formatted_record #{formatted_record.chomp}" }
            _io.write formatted_record
            @num_rows += 1
          end
          now = Time.now
          if @progress_log_timer < now - 10 # once in 10 seconds
            speed = ((@num_rows - @previous_num_rows) / (now - @progress_log_timer).to_f).round(1)
            @progress_log_timer = now
            @previous_num_rows = @num_rows
            Embulk.logger.info { "embulk-output-bigquery: num_rows #{num_format(@num_rows)} (#{num_format(speed)} rows/sec)" }
          end
          @num_rows
        end
      end
    end
  end
end
