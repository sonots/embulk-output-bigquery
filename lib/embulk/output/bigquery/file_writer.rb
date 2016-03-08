require 'zlib'
require 'json'
require 'csv'
require_relative 'value_converter_factory'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class FileWriter
        attr_reader :path

        def initialize(task, schema, index, converters = nil)
          @task = task
          @schema = schema
          @index = index
          @converters = converters || ValueConverterFactory.create_converters(task, schema)

          @num_input_rows = 0
          @progress_log_timer = Time.now
          @previous_num_input_rows = 0

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

          @path = {}
          @io   = {}
          if time_column_index = @task['time_column_index']
            table = @task['table']
            @table_proc = Proc.new {|record|
              time = record[time_column_index]
              time.strftime(table)
            }
          else
            table = @task['table']
            @table_proc = Proc.new { table }
            self.io(table)
          end
        end

        def path(table)
          return @path[table] if @path[table]
          path = sprintf("#{@task['path_prefix']}#{@task['sequence_format']}.#{table}#{@task['file_ext']}", Process.pid, index)
          Embulk.logger.info { "embulk-output-bigquery: will create #{path}" }
          if File.exist?(path)
            Embulk.logger.warn { "embulk-output-bigquery: unlink already existing #{path}" }
            File.unlink(path) rescue nil
          end
          @path[table] = path
        end

        def io(table)
          return @io[table] if @io[table]
          path = self.path(table)
          file_io = File.open(path, 'w')
          case @task['compression'].downcase
          when 'gzip'
            io = Zlib::GzipWriter.new(file_io)
          else
            io = file_io
          end
          @io[table] = io
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
          # I once tried to split IO writing into another IO thread using SizedQueue
          # However, it resulted in worse performance, so I removed the codes.
          page.each do |record|
            Embulk.logger.trace { "embulk-output-bigquery: record #{record}" }

            formatted_record = @formatter_proc.call(record)
            Embulk.logger.trace { "embulk-output-bigquery: formatted_record #{formatted_record.chomp}" }
            table = @table_proc.call(record)
            io(table).write formatted_record
            @num_input_rows += 1
          end
          now = Time.now
          if @progress_log_timer < now - 10 # once in 10 seconds
            speed = ((@num_input_rows - @previous_num_input_rows) / (now - @progress_log_timer).to_f).round(1)
            @progress_log_timer = now
            @previous_num_input_rows = @num_input_rows
            Embulk.logger.info { "embulk-output-bigquery: num_input_rows #{num_format(@num_input_rows)} (#{num_format(speed)} rows/sec)" }
          end
        end

        def commit
          @io.values.each {|io|
            io.close rescue nil
          }
          task_report = {
            'num_input_rows' => @num_input_rows,
            'path' => @path,
          }
        end
      end
    end
  end
end
