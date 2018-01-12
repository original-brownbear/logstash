require "logstash/plugin"
require "logstash/inputs/threadable"
require "logstash/namespace"

class LogStash::Inputs::Cluster < LogStash::Inputs::Base
  config_name "cluster"

  config :es_host, :validate => :string, :default => "localhost:9200"

  config :es_index, :validate => :string, :default => "lscluster"

  def register
    # start es bootstrapping loop here
  end

  def run(queue)
    # see todo section
    @wrapped_queue = org.logstash.cluster.ClusterInput.new(
        queue,
        org.logstash.cluster.elasticsearch.EsClient.create(
            org.logstash.cluster.LogstashClusterConfig.new(
                es_index, java.util.Collections.singleton(org.apache.http.HttpHost.create(es_host))
            )
        )
    )
    @wrapped_queue.run
  ensure
    @wrapped_queue.close
  end

  def stop
    @wrapped_queue.close unless @wrapped_queue.nil?
  end

  def close
    @wrapped_queue.close unless @wrapped_queue.nil?
  end
end
