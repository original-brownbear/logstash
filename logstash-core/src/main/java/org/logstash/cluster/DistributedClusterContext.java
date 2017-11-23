package org.logstash.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.UUID;

final class DistributedClusterContext implements Closeable {

    private final transient UUID nodeId = UUID.randomUUID();

    private final BlockStore blockStore;

    private final String clusterName;

    private final DistributedMap nodes;

    DistributedClusterContext(final String clusterName, final BlockStore blockStore) {
        this.clusterName = clusterName;
        this.blockStore = blockStore;
        this.nodes = new DistributedMap(this, "clusterNodes");
    }

    String getClusterName() {
        return clusterName;
    }

    BlockStore getBlockStore() {
        return blockStore;
    }

    Iterable<SlaveNode> nodes() throws IOException {
        /*
        connect();
        final Response result = esClient.performRequest(
            "GET", String.format("/logstash-cluster-%s/nodes/1", clusterName),
            Collections.emptyMap(),
            new NStringEntity("", ContentType.APPLICATION_JSON)
        );
        final int status = result.getStatusLine().getStatusCode();
        if (status != HttpStatus.SC_OK) {
            throw new IllegalStateException(
                String.format(
                    "Failed to get cluster nodes from Elasticsearch, received HTTP %d.", status
                )
            );
        }
        final Map<String, Object> data = ObjectMappers.JSON_MAPPER.readerFor(Map.class).readValue(result.getEntity().getContent());
        final Collection<String> addresses =
            (Collection<String>) ((Map<String, Object>) data.get("_source")).get("nodes");
        return addresses.stream().map(
            address -> {
                final String[] parts = address.split(":");
                return new SlaveNode(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
            }
        ).collect(Collectors.toList());
        */
        return Collections.emptyList();
    }

    void join(final InetSocketAddress address) throws IOException {
    }

    @Override
    public void close() throws IOException {
        this.blockStore.close();
    }
}
