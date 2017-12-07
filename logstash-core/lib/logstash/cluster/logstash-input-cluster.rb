require "logstash/plugin"
require "logstash/inputs/threadable"
require "logstash/namespace"

class LogStash::Inputs::Cluster < LogStash::Inputs::Base
  config_name "cluster"

  config :port, :validate => :number, :default => 8099

  config :host, :validate => :string, :default => "127.0.0.1"

  def register

  end

  # def register

  def run(queue)
    @wrapped_queue = org.logstash.plugins.input.ClusterInput.new(
        queue, org.logstash.cluster.LogstashClusterConfig.new(
        java.net.InetSocketAddress.new(host, port), java.util.Collections.empty_list,
        java.io.File.new("/tmp/lsqueue-data"))
    )
    @wrapped_queue.run
  end

  def close
    @wrapped_queue.close
  end
end
