require 'zlib'
require 'json'
require 'csv'
require_relative 'abstract_writer'
require_relative 'value_converter_factory'

module Embulk
  module Output
    class Bigquery < OutputPlugin
      class FileWriter < AbstractWriter
        def initialize(task, schema, index, converters = nil)
          super(task, schema, index, converters)
        end

        def path
          io.path
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
          Embulk.logger.debug { "close #{io.path}" }
          io.close rescue nil
          io
        end

        def reopen
          @io = open(io.path, 'a')
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
          log_num_rows
          @num_rows
        end
      end
    end
  end
end
