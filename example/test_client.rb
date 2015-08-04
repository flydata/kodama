def append_load_path_if_not_exist(new_lib_path)
  absolute_path = File.realpath(new_lib_path)
  $LOAD_PATH.unshift absolute_path unless $LOAD_PATH.include? absolute_path
end

kodama_top_dir = File.realpath('../../', __FILE__)
kodama_lib_dir = File.realpath('lib', kodama_top_dir)
append_load_path_if_not_exist(kodama_lib_dir)

require 'kodama'
require 'json'
require 'optparse'

option={}
OptionParser.new do |opt|
  opt.on('-u VALUE', 'username')       {|v| option[:username] = v}
  opt.on('-p VALUE', 'password')       {|v| option[:password] = v}
  opt.on('-h VALUE', 'host')         {|v| option[:host] = v}
  opt.on('--port VALUE', 'port')     {|v| option[:port] = v.to_i}
  opt.on('--ssl-ca VALUE', 'ssl_ca') {|v| option[:ssl_ca] = v}
  opt.on('--ssl-cipher VALUE', 'ssl_cipher') {|v| option[:ssl_cipher] = v}
  # --help for showing help
  opt.parse!(ARGV)
end

conn_info = {
  username: (option[:username] || 'root'),
  host: (option[:host] || '127.0.0.1'),
  port: (option[:port] || 3306),
}
conn_info[:password] = option[:password] if option[:password]


class Worker
  def initialize
  end

  def perform(event)
    record = get_row(event)
    puts "record: #{record}"
  end

  def get_row(event)
    puts "event: #{event.event_type}"
    case event.event_type
    when /Write/, /Delete/, /Update/
      event.rows
    end
  end
end


SLEEP_TIME = 3
def start_kodama(conn_info, option)
  worker = Worker.new
  puts "===== Kodama client start ====="
  Kodama::Client.start(conn_info) do |c|
    puts conn_info
    puts "== Start connecting to #{conn_info.merge(password: (conn_info[:password].nil? ? '<not set>' : 'xxxx' ))}"
    c.binlog_position_file = 'position.log'
    c.sent_binlog_position_file = 'sent_position.log'
    if option[:ssl_ca]
      c.ssl_ca = option[:ssl_ca]
      c.ssl_cipher = option[:ssl_cipher]
    end

    c.on_row_event do |event|
      worker.perform(event)
      sleep SLEEP_TIME
    end
    c.on_query_event do |event|
      puts "query: #{event.query}"
      sleep SLEEP_TIME
    end
    c.on_rotate_event do |event|
      puts "rotate:#{event.binlog_file} #{event.binlog_pos}"
      sleep SLEEP_TIME
    end
    c.on_table_map_event do |event|
      puts "table_map: #{event.table_name}"
      sleep SLEEP_TIME
    end
  end
end

begin
  start_kodama(conn_info, option)
rescue => e
  puts "Error!:#{e}"
end
