require_relative './helper'
require 'embulk/output/bigquery/bigquery_client'
require 'csv'

# 1. Prepare example/your-project-000.json
# 2. bunlde exec ruby test/test_bigquery_client.rb

unless File.exist?(JSON_KEYFILE)
  puts "#{JSON_KEYFILE} is not found. Skip test/test_bigquery_client.rb"
else
  module Embulk
    class Output::Bigquery
      class TestBigqueryClient < Test::Unit::TestCase
        class << self
          def startup
            FileUtils.mkdir_p('tmp')
          end

          def shutdown
            FileUtils.rm_rf('tmp')
          end
        end

        def client(task = {})
          task = least_task.merge(task)
          BigqueryClient.new(task, schema)
        end

        def least_task
          {
            'project'          => JSON.parse(File.read(JSON_KEYFILE))['project_id'],
            'dataset'          => 'your_dataset_name',
            'table'            => 'your_table_name',
            'auth_method'      => 'json_key',
            'json_keyfile'     => JSON_KEYFILE,
            'retries'          => 3,
            'timeout_sec'      => 300,
            'open_timeout_sec' => 300,
            'job_status_max_polling_time' => 3600,
            'job_status_polling_interval' => 10,
            'source_format'    => 'CSV',
            'path_prefix'      => 'tmp/a',
            'compression'      => 'GZIP',
          }
        end

        def schema
          Schema.new([
            Column.new({index: 0, name: 'boolean', type: :boolean}),
            Column.new({index: 1, name: 'long', type: :long}),
            Column.new({index: 2, name: 'double', type: :double}),
            Column.new({index: 3, name: 'string', type: :string}),
            Column.new({index: 4, name: 'timestamp', type: :timestamp}),
            Column.new({index: 5, name: 'json', type: :json}),
          ])
        end

        def record
          [true,1,1.1,'1',Time.parse("2016-02-26 +00:00"),'{"foo":"bar"}']
        end

        sub_test_case "client" do
          def test_json_keyfile
            assert_nothing_raised { BigqueryClient.new(least_task, schema).client }
          end

          def test_p12_keyfile
            # pending
          end
        end

        sub_test_case "create_dataset" do
          def test_create_dataset
            assert_nothing_raised { client.create_dataset }
          end

          def test_create_dataset_with_reference
            response = client.get_dataset
            any_instance_of(BigqueryClient) do |obj|
              mock(obj).get_dataset('your_dataset_name') { response }
            end
            assert_nothing_raised do
              client.create_dataset('your_dataset_name_old', reference: 'your_dataset_name')
            end
          end
        end

        sub_test_case "get_dataset" do
          def test_get_dataset
            assert_nothing_raised { client.create_dataset }
            assert_nothing_raised { client.get_dataset }
          end

          def test_get_dataset_not_found
            assert_raise(NotFoundError) {
              client.get_dataset('something_does_not_exist')
            }
          end
        end

        sub_test_case "create_table" do
          def test_create_table
            client.delete_table('your_table_name')
            assert_nothing_raised { client.create_table('your_table_name') }
          end

          def test_create_table_already_exists
            assert_nothing_raised { client.create_table('your_table_name') }
          end
        end

        sub_test_case "delete_table" do
          def test_delete_table
            client.create_table('your_table_name')
            assert_nothing_raised { client.delete_table('your_table_name') }
          end

          def test_delete_table_not_found
            assert_nothing_raised { client.delete_table('your_table_name') }
          end
        end

        sub_test_case "get_table" do
          def test_get_table
            client.create_table('your_table_name')
            assert_nothing_raised { client.get_table('your_table_name') }
          end

          def test_get_table_not_found
            client.delete_table('your_table_name')
            assert_raise(NotFoundError) {
              client.get_table('your_table_name')
            }
          end
        end

        sub_test_case "fields" do
          def test_fields_from_table
            client.create_table('your_table_name')
            fields = client.fields_from_table('your_table_name')
            expected = [
              {:type=>"BOOLEAN", :name=>"boolean"},
              {:type=>"INTEGER", :name=>"long"},
              {:type=>"FLOAT", :name=>"double"},
              {:type=>"STRING", :name=>"string"},
              {:type=>"TIMESTAMP", :name=>"timestamp"},
              {:type=>"STRING", :name=>"json"},
            ]
            assert_equal expected, fields
          end
        end

        sub_test_case "copy" do
          def test_create_table
            client.create_table('your_table_name')
            assert_nothing_raised { client.copy('your_table_name', 'your_table_name_old') }
          end
        end

        sub_test_case "load" do
          def test_load
            client.create_table('your_table_name')
            File.write("tmp/your_file_name.csv", record.to_csv)
            assert_nothing_raised { client.load("/tmp/your_file_name.csv", 'your_table_name') }
          end
        end

        sub_test_case "cat_file" do
          def setup
            @from_files = %w[tmp/from_file1.csv tmp/from_file2.csv]
            @from_files.each {|file| File.write(file, record.to_csv) }
            @to_file = 'tmp/to_file.csv'
          end

          def teardown
            (@from_files + [@to_file]).each{|file| File.unlink(file) rescue nil }
          end

          def test_cat_file
            @from_files.each do |from_file|
              client.send('cat_file', from_file, @to_file)
            end
            to_content = File.read(@to_file)
            from_content = @from_files.map {|file| File.read(file) }.join('')
            assert { to_content == from_content }
          end
        end

        sub_test_case "cat_files" do
          def setup
            @from_files = %w[
              tmp/from_file1.csv
              tmp/from_file2.csv
              tmp/from_file3.csv
              tmp/from_file4.csv
              tmp/from_file5.csv
            ]
            @from_files.each {|file| File.write(file, record.to_csv) }
            @to_file = 'tmp/to_file.csv'
          end

          def teardown
            (@from_files + [@to_file]).each{|file| File.unlink(file) rescue nil }
          end

          def test_cat_files
            file_map = client.send('cat_files', @from_files, 2)
            groups = file_map.group_by {|from_file, to_file| to_file }
            assert { groups.size == 2 }
            assert { groups.values[0].size == 3 }
            assert { groups.values[1].size == 2 }
            groups.each do |to_file, file_map_in_group|
              from_files = file_map_in_group.map(&:first)
              to_content = File.read(to_file)
              from_content = from_files.map {|file| File.read(file) }.join('')
              assert { to_content == from_content }
            end
          end
        end
      end
    end
  end
end
