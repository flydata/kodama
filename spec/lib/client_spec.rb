require 'spec_helper'

describe Kodama::Client do
  describe '.mysql_url' do
    def mysql_url(options)
      Kodama::Client.mysql_url(options)
    end

    it do
      mysql_url(:username => 'user', :host => 'example.com').should == 'mysql://user@example.com'
      mysql_url(:username => 'user', :host => 'example.com',
                :password => 'password', :port => 3306).should == 'mysql://user:password@example.com:3306'
    end
  end

  describe '#start' do
    class TestBinlogClient
      attr_accessor :connect
      def initialize(events = [], connect = true)
        @events = events
        @connect = connect
      end

      def wait_for_next_event
        event = @events.shift
        stub_event(event)
        event
      end

      def closed?
        true
      end

      def stub_event(target_event)
        target_event_class = target_event.instance_variable_get('@name')
        [Binlog::QueryEvent, Binlog::RowEvent, Binlog::RotateEvent, Binlog::Xid].each do |event|
          if event == target_event_class
            event.stub(:===).and_return { true }
          else
            # :=== is stubbed
            if event.method(:===).owner != Module
              event.unstub(:===)
            end
          end
        end
      end
    end

    class TestPositionFile
      def update(filename, position)
      end

      def read
      end
    end

    let(:connect) { true }
    let(:binlog_client) { TestBinlogClient.new(events, connect) }

    def stub_position_file(position_file = nil)
      client.stub(:position_file).and_return { position_file || TestPositionFile.new }
    end

    let(:client) {
      c = Kodama::Client.new('mysql://user@host')
      c.stub(:binlog_client).and_return { binlog_client }
      c
    }

    let(:rotate_event) do
      mock(Binlog::RotateEvent).tap do |event|
        event.stub(:next_position).and_return { 0 }
        event.stub(:binlog_file).and_return { 'binlog' }
        event.stub(:binlog_pos).and_return { 100 }
      end
    end

    let(:query_event) do
      mock(Binlog::QueryEvent).tap do |event|
        event.stub(:next_position).and_return { 200 }
      end
    end

    let(:row_event) do
      mock(Binlog::RowEvent).tap do |event|
        event.stub(:next_position).and_return { 300 }
      end
    end

    let(:xid_event) do
      mock(Binlog::Xid).tap do |event|
        event.stub(:next_position).and_return { 400 }
      end
    end

    context "with query event" do
      let(:events) { [query_event] }
      it 'should receive query_event' do
        expect {|block|
          client.on_query_event(&block)
          client.start
        }.to yield_with_args(query_event)
      end
    end

    context "with row event" do
      let(:events) { [row_event] }
      it 'should receive row_event' do
        expect {|block|
          client.on_row_event(&block)
          client.start
        }.to yield_with_args(row_event)
      end
    end

    context "with multiple events" do
      let(:events) { [rotate_event, query_event, row_event, xid_event] }
      it 'should save position only on row, query and rotate event' do
        position_file = TestPositionFile.new.tap do |pf|
          pf.should_receive(:update).with('binlog', 100).once.ordered
          pf.should_receive(:update).with('binlog', 200).once.ordered
          pf.should_receive(:update).with('binlog', 300).once.ordered
        end
        stub_position_file(position_file)
        client.binlog_position_file = 'test'
        client.start
      end
    end

    context "when connection failed" do
      let(:events) { [query_event] }
      let(:connect) { false }
      it 'should retry exactly specifeid times' do
        client.connection_retry_limit = 2
        client.connection_retry_wait = 0.1
        expect { client.start }.to raise_error(Binlog::Error)
        client.connection_retry_count.should == 2
      end
    end

    context "when an error is raised" do
      let(:events) { [query_event] }
      let(:retry_limit) { 2 }
      before do
        client.connection_retry_limit = retry_limit
        client.connection_retry_wait = 0.1
      end
      it 'should retry exactly specifeid times' do
        binlog_client.should_receive(:wait_for_next_event).exactly(retry_limit + 1).times.and_raise(Binlog::Error)
        expect { client.start }.to raise_error(Binlog::Error)
        client.connection_retry_count.should == retry_limit
      end
    end

    context "with stop request" do
      let(:events) { [query_event, row_event] }
      it 'should stop when it receives stop request' do
        client.on_query_event do |event|
          self.stop_request
        end
        expect {|block| client.on_row_event(&block) }.not_to yield_control
        client.start
      end
    end
  end
end
