require "logstash/plugin"
require "logstash/inputs/threadable"
require "logstash/namespace"

class LogStash::Inputs::Cluster < LogStash::Inputs::Threadable
  config_name "cluster"

  default :codec, "plain"

  config :message, :validate => :string, :default => "Hello Cluster!"

  config :lines, :validate => :array

  config :count, :validate => :number, :default => 0

  public
  def register
    @host = Socket.gethostname
    @count = Array(@count).first
  end

  # def register

  def run(queue)
    number = 0

    if @message == "stdin"
      @logger.info("Generator plugin reading a line from stdin")
      @message = $stdin.readline
      @logger.debug("Generator line read complete", :message => @message)
    end
    @lines = [@message] if @lines.nil?

    while !stop? && (@count <= 0 || number < @count)
      @lines.each do |line|
        @codec.decode(line.clone) do |event|
          decorate(event)
          event.set("host", @host)
          event.set("sequence", number)
          queue << event
        end
      end
      number += 1
    end # loop

    if @codec.respond_to?(:flush)
      @codec.flush do |event|
        decorate(event)
        event.set("host", @host)
        queue << event
      end
    end
  end

  public
  def close
    if @codec.respond_to?(:flush)
      @codec.flush do |event|
        decorate(event)
        event.set("host", @host)
        queue << event
      end
    end
  end
end
