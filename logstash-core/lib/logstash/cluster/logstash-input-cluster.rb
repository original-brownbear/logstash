require "logstash/plugin"
require "logstash/inputs/threadable"
require "logstash/namespace"

# todo: Bootstrap from ES
# todo: Backup state to ES
# todo: Create real master plugin
class LogStash::Inputs::Cluster < LogStash::Inputs::Base
  config_name "cluster"

  config :bind_port, :validate => :number, :default => 8099

  config :bind_host, :validate => :string, :default => "127.0.0.1"

  config :node_id, :validate => :string, :default => "node1"

  config :es_host, :validate => :string, :default => "localhost:9200"

  config :es_index, :validate => :string, :default => "lscluster"

  config :data_path, :validate => :string, :default => "/tmp/lsqueue-data"

  def register
    # start es bootstrapping loop here
  end

  def run(queue)
    # see todo section
    @wrapped_queue = org.logstash.cluster.ClusterInput.new(
        queue,
        org.logstash.cluster.ClusterConfigProvider.esConfigProvider(
            org.elasticsearch.transport.client.PreBuiltTransportClient.new
                .add_transport_address(
                    org.elasticsearch.common.transport.TransportAddress.new(
                        java.net.InetAddress.localHost, 9200
                    )
                ),
            org.logstash.cluster.LogstashClusterConfig.new(
                node_id, java.net.InetSocketAddress.new(bind_host, bind_port),
                java.util.Collections.empty_list, java.io.File.new(data_path)
            )
        )
    )
    @wrapped_queue.run
  end

  def stop
    @wrapped_queue.close unless @wrapped_queue.nil?
  end

  def close
    @wrapped_queue.close unless @wrapped_queue.nil?
  end
end
