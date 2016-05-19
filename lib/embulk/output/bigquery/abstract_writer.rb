require 'zlib'
require 'json'
require 'csv'
require_relative 'value_converter_factory'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class AbstractWriter
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

        def log_num_rows
          now = Time.now
          if @progress_log_timer < now - 10 # once in 10 seconds
            speed = ((@num_rows - @previous_num_rows) / (now - @progress_log_timer).to_f).round(1)
            @progress_log_timer = now
            @previous_num_rows = @num_rows
            Embulk.logger.info { "embulk-output-bigquery: num_rows #{num_format(@num_rows)} (#{num_format(speed)} rows/sec)" }
          end
        end

        def close
          raise NotImplementedError
        end

        def add(page)
          raise NotImplementedError
        end
      end
    end
  end
end
