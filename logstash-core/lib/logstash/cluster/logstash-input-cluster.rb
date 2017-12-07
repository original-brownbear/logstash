require "logstash/plugin"
require "logstash/inputs/threadable"
require "logstash/namespace"

class LogStash::Inputs::Cluster < LogStash::Inputs::Threadable
  config_name "cluster"

  config :port, :validate => :number, :default => 8099

  def register

  end

  # def register

  def run(queue)
    @wrapped_queue = org.logstash.plugins.input.ClusterInput.new(
        queue, org.logstash.cluster.LogstashClusterConfig.new(
        java.net.InetSocketAddress.new("127.0.0.1", 8099), java.util.Collections.empty_list,
        java.io.File.new("/tmp/data"))
    )
    @wrapped_queue.run
  end

  def close
    @wrapped_queue.close
  end
end
