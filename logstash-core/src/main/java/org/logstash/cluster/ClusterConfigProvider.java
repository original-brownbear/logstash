package org.logstash.cluster;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import org.elasticsearch.client.Client;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.elasticsearch.EsClient;

public interface ClusterConfigProvider extends Closeable {

    LogstashClusterConfig currentConfig();

    void publishBootstrapNodes(Collection<Node> nodes);

    static ClusterConfigProvider esConfigProvider(final Client client,
        final LogstashClusterConfig defaults) throws ExecutionException, InterruptedException {
        return new EsClient(client, defaults);
    }
}
