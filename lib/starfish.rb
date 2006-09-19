require 'rinda/ring'
require 'rinda/tuplespace'
require 'timeout'
require 'logger'
require 'md5'
require 'yaml'
require 'tmpdir'

require 'map_reduce'

DRb.start_service

if RUBY_PLATFORM =~ /mswin/
  begin
    require "win32-process"
    STARFISH_FORK = Win32::Process.method(:fork)
  rescue
    $stderr.puts "Your Windows system does not support forking, please install http://raa.ruby-lang.org/project/win32-process/"
  end
else
  STARFISH_FORK = method(:fork)
end

class StarfishError < StandardError; end
class Starfish
  VERSION = "1.1.3"
  NULL = (RUBY_PLATFORM =~ /mswin/) ? 'NUL' : '/dev/null'
  
  @@server = false
  @@client = false
  @@options = {
    :log => "#{Dir.tmpdir}/#{File.basename(ARGV.first)}.log"
  }
  
  include Rinda
  attr_accessor :started, :ring_server, :server
  
  # set the uniq identifier
  def initialize(uniq=ARGV.first)
    @retry_count = 0
    @uniq = uniq
  end
  
  def uniq
    MD5.new(@uniq).to_s
  end
  
  def server
    unless @@server
      $stderr.puts "You must specify a server"
      exit
    end

    map_reduce = MapReduce.new
    object = @@server.call(map_reduce)
    
    if map_reduce.valid?
      object = map_reduce
    end
    
    sanitize object
    
    ts = Rinda::TupleSpace.new
    begin
      ts.write([:name, uniq.intern, object, @uniq])
      ring_server = Rinda::RingServer.new(ts)
    rescue Errno::EADDRINUSE
      ts = RingFinger.primary
      ts.write([:name, uniq.intern, object, @uniq])
    end

    File.open(@@options[:pid] || "#{Dir.tmpdir}/starfish-#{uniq}.pid","w"){|f|f<<Process.pid}

    $stderr.puts "server started for #{object.inspect}"
    
    DRb.thread.join
  end
  
  def client
    unless @@client
      $stderr.puts "You must specify a client"
      exit
    end

    negotiate
    Timeout::timeout(5) { @server_object = @ring_server.read([:name, uniq.intern, nil, nil])[2] }
    
    i = 0
    loop do
      i += 1
      GC.start if i%1000 == 0
      
      begin
        @server_object.map_reduce?
      rescue NoMethodError
        @called = @@client.call(@server_object)
      end
      
      begin
        unless @called
          if @server_object.map_reduce? && @server_object.valid?
            map_reduce_client = eval("MapReduce::#{@server_object.base_type_to_s}::Client").new(@server_object)

            $server_object = @server_object
            Object.instance_eval do
              define_method(:logger) do |*args|
                $server_object._logger(*args)
              end
              define_method(:server) do
                $server_object
              end
            end
        
            map_reduce_client.each do |object|
              t = Time.now
              Timeout::timeout(@@options[:timeout] || 60) do
                @@client.call(object)
              end
              @server_object.add_time_spent_processing_objects(Time.now-t)
            end
          else
            raise MapReduceError, "invalid map reduce server (possibly missing type or input)"
          end
        end
      rescue NoMethodError
      end
    end
    
  rescue Timeout::Error => m
    spawn unless @called
    @retry_count += 1
    if @retry_count <= 5
      retry
    else
      raise Timeout::Error, m
    end
  rescue DRb::DRbConnError => m
    stop
    negotiate
    @retry_count += 1
    if @retry_count <= 5
      retry
    else
      raise DRb::DRbConnError, m
    end
  end
  
  def stats
    negotiate
    Timeout::timeout(5) { @server_object = @ring_server.read([:name, uniq.intern, nil, nil])[2] }
    
    puts @server_object.stats.to_yaml
  rescue NoMethodError
    $stderr.puts "The stats method is not defined for your server"
  end
  
  def sanitize(object)
    object.extend DRbUndumped
    
    @@log = case @@options[:log]
    when String
      Logger.new(@@options[:log])
    when Class
      @@options[:log].new
    when nil, false
      Logger.new(NULL)
    else
      @@options[:log]
    end
    
    def object.logger
      @logger ||= @@log
    end
    def object._logger
      @logger ||= @@log
    end
  end
  
  def negotiate
    begin
      @ring_server = RingFinger.primary
    rescue RuntimeError => m
      # allow multiple un-cached calls to RingFinger.finger
      def RingFinger.finger
        @@finger = self.new
        @@finger.lookup_ring_any
        @@finger
      end

      spawn
      @retry_count += 1
      if @retry_count <= 5
        retry
      else
        raise RuntimeError, m
      end
    end
  end

  def spawn
    @started ||= STARFISH_FORK.call { system("ruby #{File.dirname(__FILE__)}/../bin/starfish #{@uniq} server > #{NULL}") }
  end
  
  def stop
    puts "stopping the server"
    Process.kill("SIGHUP", IO.read(@@options[:pid] || "#{Dir.tmpdir}/starfish-#{uniq}.pid").to_i)
  rescue Errno::ENOENT
    puts "Fatal error, please kill all starfish processes manually and try again"
    system("ps auxww|grep starfish")
  end
  
  class << self
    def server=(server)
      @@server = server
    end
    
    def client=(client)
      @@client = client
    end
    
    def options=(options={})
      @@options.update(options)
    end
  end
end