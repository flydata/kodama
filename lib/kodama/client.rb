# coding: utf-8

require 'binlog'
require 'logger'

module Kodama
  class Client
    LOG_LEVEL = {
      :fatal => Logger::FATAL,
      :error => Logger::ERROR,
      :warn => Logger::WARN,
      :info => Logger::INFO,
      :debug => Logger::DEBUG,
    }

    class << self
      def start(options = {}, &block)
        client = self.new(mysql_url(options))
        block.call(client)
        client.start
      end

      def mysql_url(options = {})
        password = options[:password] ? ":#{options[:password]}" : nil
        port = options[:port] ? ":#{options[:port]}" : nil
        "mysql://#{options[:username]}#{password}@#{options[:host]}#{port}"
      end
    end

    def initialize(url)
      @url = url
      @binlog_info = BinlogInfo.new
      @sent_binlog_info = BinlogInfo.new
      @retry_info = RetryInfo.new(:limit => 100, :wait => 3)
      @callbacks = {}
      @logger = Logger.new(STDOUT)
      @safe_to_stop = true

      self.log_level = :info
    end

    def on_query_event(&block); @callbacks[:on_query_event] = block; end
    def on_rotate_event(&block); @callbacks[:on_rotate_event] = block; end
    def on_int_var_event(&block); @callbacks[:on_int_var_event] = block; end
    def on_user_var_event(&block); @callbacks[:on_user_var_event] = block; end
    def on_format_event(&block); @callbacks[:on_format_event] = block; end
    def on_xid(&block); @callbacks[:on_xid] = block; end
    def on_table_map_event(&block); @callbacks[:on_table_map_event] = block; end
    def on_row_event(&block); @callbacks[:on_row_event] = block; end
    def on_incident_event(&block); @callbacks[:on_incident_event] = block; end
    def on_unimplemented_event(&block); @callbacks[:on_unimplemented_event] = block; end

    def binlog_position_file=(filename)
      @position_file = position_file(filename)
      @binlog_info.load!(@position_file)
    end

    def sent_binlog_position_file=(filename)
      @sent_position_file = position_file(filename)
      @sent_binlog_info.load!(@sent_position_file)
    end

    def connection_retry_wait=(wait)
      @retry_info.wait = wait
    end

    def connection_retry_limit=(limit)
      @retry_info.limit = limit
    end

    def log_level=(level)
      @logger.level = LOG_LEVEL[level]
    end

    def gracefully_stop_on(*signals)
      signals.each do |signal|
        Signal.trap(signal) do
          if safe_to_stop?
            exit(0)
          else
            stop_request
          end
        end
      end
    end

    def binlog_client(url)
      Binlog::Client.new(url)
    end

    def position_file(filename)
      PositionFile.new(filename)
    end

    def connection_retry_count
      @retry_info.count
    end

    def safe_to_stop?
      !!@safe_to_stop
    end

    def stop_request
      @stop_requested = true
    end

    def start
      @retry_info.count_reset
      begin
        client = binlog_client(@url)
        raise Binlog::Error, 'MySQL server has gone away' unless client.connect

        if @binlog_info.valid?
          client.set_position(@binlog_info.filename, @binlog_info.position)
        end

        while event = client.wait_for_next_event
          unsafe do
            process_event(event)
          end
          break if stop_requested?
        end
      rescue Binlog::Error => e
        @logger.debug e
        if client.closed? && @retry_info.retryable?
          sleep @retry_info.wait
          @retry_info.count_up
          retry
        end
        raise e
      end
    end

    private
    def unsafe
      @safe_to_stop = false
      yield
      @safe_to_stop = true
    end

    def stop_requested?
      @stop_requested
    end

    def process_event(event)
      # If the position in binlog file is behind the sent position,
      # keep updating only binlog info in most of cases
      processable = @binlog_info.should_process?(@sent_binlog_info)

      # Keep current binlog position temporary
      cur_binlog_file = @binlog_info.filename
      cur_binlog_pos = @binlog_info.position

      case event
      when Binlog::QueryEvent
        if processable
          callback :on_query_event, event
          # Save current event's position as sent (@sent_binlog_info)
          @sent_binlog_info.save_with(cur_binlog_file, cur_binlog_pos)
        end
        # Save next event's position as checkpoint (@binlog_info)
        @binlog_info.save_with(cur_binlog_file, event.next_position)

      when Binlog::RotateEvent
        # Even if the event is already sent, call callback
        # because app might need binlog info when resuming.
        callback :on_rotate_event, event
        # Update binlog_info with rotation
        @binlog_info.save_with(event.binlog_file, event.binlog_pos)

      when Binlog::IntVarEvent
        if processable
          callback :on_int_var_event, event
        end

      when Binlog::UserVarEvent
        if processable
          callback :on_user_var_event, event
        end

      when Binlog::FormatEvent
        if processable
          callback :on_format_event, event
        end

      when Binlog::Xid
        if processable
          callback :on_xid, event
        end

      when Binlog::TableMapEvent
        if processable
          callback :on_table_map_event, event
        end
        # Save current event's position as checkpoint
        @binlog_info.save_with(cur_binlog_file, cur_binlog_pos)

      when Binlog::RowEvent
        if processable
          callback :on_row_event, event
          # Save current event's position as sent
          @sent_binlog_info.save_with(cur_binlog_file, cur_binlog_pos)
        end

      when Binlog::IncidentEvent
        if processable
          callback :on_incident_event, event
        end

      when Binlog::UnimplementedEvent
        if processable
          callback :on_unimplemented_event, event
        end

      else
        @logger.error "Not Implemented: #{event.event_type}"
      end

      # Set the next event position for the next iteration
      set_next_event_position(@binlog_info, event)
    end

    def callback(name, *args)
      if @callbacks[name]
        instance_exec *args, &@callbacks[name]
      else
        @logger.debug "Unhandled: #{name}"
      end
    end

    # Set the next event position for the next iteration
    # Compare positions to avoid decreasing the position unintentionally
    # because next_position of Binlog::FormatEvent is always 0
    def set_next_event_position(binlog_info, event)
      if !event.kind_of?(Binlog::RotateEvent) && binlog_info.position.to_i < event.next_position.to_i
        binlog_info.position = event.next_position
      end
    end

    class BinlogInfo
      attr_accessor :filename, :position, :position_file

      def initialize(filename = nil, position = nil, position_file = nil)
        @filename = filename
        @position = position
        @position_file = position_file
      end

      def valid?
        @filename && @position
      end

      def save_with(filename, position)
        @filename = filename if filename
        @position = position if position
        save
      end

      def save(position_file = nil)
        @position_file = position_file if position_file
        @position_file.update(@filename, @position) if @position_file
      end

      def load!(position_file = nil)
        @position_file = position_file if position_file
        @filename, @position = @position_file.read
      end

      def should_process?(sent_binlog_info)
        if self.valid? && sent_binlog_info && sent_binlog_info.valid?
          # Compare binlog filename and position
          #
          # Event should be sent only when the event position is bigger than
          # the sent position
          #
          #   ex)
          #   binlog_info               sent_binlog_info     result
          #   -----------------------------------------------------------
          #   mysql-bin.000004 00001    mysql-bin.000003 00001    true
          #   mysql-bin.000004 00030    mysql-bin.000004 00001    true
          #   mysql-bin.000004 00030    mysql-bin.000004 00030    false
          #   mysql-bin.000004 00030    mysql-bin.000004 00050    false
          #   mysql-bin.000004 00030    mysql-bin.000005 00001    false
          @filename > sent_binlog_info.filename ||
            (@filename == sent_binlog_info.filename &&
             @position.to_i > sent_binlog_info.position.to_i)
        else
          true
        end
      end
    end

    class RetryInfo
      attr_accessor :count, :limit, :wait
      def initialize(options = {})
        @wait = options[:wait] || 3
        @limit = options[:limit] || 100
        @count = 0
      end

      def retryable?
        @count < @limit
      end

      def count_up
        @count += 1
      end

      def count_reset
        @count = 0
      end
    end

    class PositionFile
      def initialize(filename)
        @file = open(filename, File::RDWR|File::CREAT)
        @file.sync = true
      end

      def update(filename, position)
        @file.pos = 0
        @file.write "#{filename}\t#{position}"
        @file.truncate @file.pos
      end

      def read
        @file.pos = 0
        if line = @file.gets
          filename, position = line.split("\t")
          [filename, position.to_i]
        end
      end
    end
  end
end
