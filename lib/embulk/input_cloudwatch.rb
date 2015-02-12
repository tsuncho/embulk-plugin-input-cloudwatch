module Embulk
  module Plugin
    class InputCloudwatch < InputPlugin
      Plugin.register_input('cloudwatch', self)

      def self.transaction(config, &control)


        task = {
          :aws_key_id => config.param('aws_key_id', :string, default: nil),
          :aws_sec_key => config.param('aws_sec_key', :string, default: nil),
          :cw_endpoint => config.param('cw_endpoint', :string),
          :namespace => config.param('namespace', :string),
          :statistics => config.param('statistics', :string, default: "Average"),
          :dimensions_name => config.param('dimensions_name', :string, default: nil),
          :dimensions_value => config.param('dimensions_value', :string, default: nil),
          :period => config.param('period', :integer, default: 300),
          :open_timeout => config.param('open_timeout', :integer, default: 10),
          :read_timeout => config.param('read_timeout', :integer, default: 30),
          :start_time => config.param('start_time', :string, default: nil),
          :end_time => config.param('end_time', :string, default: nil),
          :metric_name => config.param('metric_name', :string).split(","),
          :utc_offset => config.param('utc_offset', :string, default: "+00:00"),
        }

        columns = [
          Column.new(0, "namespace", :string),
          Column.new(1, "dimension_name", :string),
          Column.new(2, "dimension_value", :string),
          Column.new(3, "metric_name", :string),
          Column.new(4, "timestamp", :timestamp),
          Column.new(5, "statistics", :string),
          Column.new(6, "count", :double),
        ]

        threads = task[:metric_name].length

        commit_reports = yield(task, columns, threads)
        return commit_reports
      end

      def initialize(task, schema, index, page_builder)
        super

        require 'aws-sdk'
        AWS.config(
          :http_open_timeout => @open_timeout,
          :http_read_timeout => @read_timeout,
        )

        @aws_sec_key = task['aws_sec_key']
        @cw_endpoint = task['cw_endpoint']
        @namespace = task['namespace']
        @statistics = task['statistics']
        @period = task['period']
        @open_timeout = task['open_timeout']
        @read_timeout = task['read_timeout']

        @dimensions_name = task['dimensions_name']
        @dimensions_value = task['dimensions_value']
        @dimensions = []
        if @dimensions_name && @dimensions_value
          names = @dimensions_name.split(",").each
          values = @dimensions_value.split(",").each
          loop do
            @dimensions.push({
              :name => names.next,
              :value => values.next,
            })
          end
        else
          @dimensions.push({
            :name => @dimensions_name,
            :value => @dimensions_value,
          })
        end

        start_time = task['start_time']
        end_time = task['end_time']
        if end_time then
          end_time_t = Time.parse(end_time) 
          @end_time = end_time_t.iso8601
        else
          end_time_t = Time.now 
          @end_time = end_time_t.iso8601
        end
        if start_time then
          @start_time = Time.parse(start_time).iso8601
        else
          @start_time = (end_time_t - @period*10).iso8601
        end

        @metric_name = task["metric_name"][index]

        @utc_offset = task["utc_offset"]
      end

      def run
        @cw = AWS::CloudWatch.new(
          :access_key_id        => @aws_key_id,
          :secret_access_key    => @aws_sec_key,
          :cloud_watch_endpoint => @cw_endpoint,
        ).client

        statistics = @cw.get_metric_statistics({
          :namespace   => @namespace,
          :metric_name => @metric_name,
          :statistics  => [@statistics],
          :dimensions  => @dimensions,
          :start_time  => @start_time,
          :end_time    => @end_time,
          :period      => @period,
        })

        statistics[:datapoints].each {|datapoint|
          output_data = [
            @namespace,
            @dimensions_name,
            @dimensions_value,
            @metric_name,
            datapoint[:timestamp].getlocal(@utc_offset),
            @statistics,
            datapoint[@statistics.downcase.to_sym],
          ]

          @page_builder.add(output_data)
        }

        @page_builder.finish
        return {}
      end

    end
  end
end
