require "logstash/plugin"
require "logstash/inputs/threadable"
require "logstash/namespace"

class LogStash::Inputs::Cluster < LogStash::Inputs::Base
  config_name "cluster"

  config :bind_port, :validate => :number, :default => 8099

  config :bind_host, :validate => :string, :default => "127.0.0.1"

  config :node_id, :validate => :string, :default => "node1"

  config :es_host, :validate => :string, :default => "localhost:9200"

  config :es_index, :validate => :string, :default => "lscluster"

  config :data_path, :validate => :string, :default => "/tmp/lsqueue-data"

  def register

  end

  def run(queue)
    @wrapped_queue = org.logstash.plugins.input.ClusterInput.new(
        queue, org.logstash.cluster.LogstashClusterConfig.new(
        node_id, java.net.InetSocketAddress.new(bind_host, bind_port),
        java.util.Collections.empty_list, java.io.File.new(data_path))
    )
    @wrapped_queue.run
  end

  def close
    @wrapped_queue.close
  end
end