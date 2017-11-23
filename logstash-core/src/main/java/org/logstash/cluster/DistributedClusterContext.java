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

    private final DistributedSet nodes;

    DistributedClusterContext(final String clusterName, final BlockStore blockStore) {
        this.clusterName = clusterName;
        this.blockStore = blockStore;
        this.nodes = new DistributedSet(this, "clusterNodes");
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
        /*
        connect();
        final Map<String, Object> data = new HashMap<>();
        final Map<String, Object> script = new HashMap<>();
        final Map<String, Object> params = new HashMap<>();
        script.put(
            "source",
            String.join(
                " ",
                "if (ctx._source.nodes.contains(params.address)) { ctx.op = 'none' } else",
                "{ ctx._source.nodes.add(params.address)}"
            )
        );
        script.put("lang", "painless");
        script.put("params", params);
        params.put("address", address);
        data.put("script", script);
        final Map<String, Object> upsert = new HashMap<>();
        upsert.put("nodes", Collections.singletonList(address));
        data.put("upsert", upsert);
        final String json;
        try {
            json = ObjectMappers.JSON_MAPPER.writeValueAsString(data);
        } catch (final JsonProcessingException ex) {
            throw new IllegalStateException(ex);
        }
        final int result = esClient.performRequest(
            "POST", String.format("/logstash-cluster-%s/nodes/1/_update", clusterName),
            Collections.emptyMap(),
            new NStringEntity(json, ContentType.APPLICATION_JSON)
        ).getStatusLine().getStatusCode();
        if (result != HttpStatus.SC_CREATED && result != HttpStatus.SC_OK) {
            throw new IllegalStateException(
                String.format(
                    "Failed to save cluster join to Elasticsearch, received HTTP %d.", result
                )
            );
        }*/
    }

    @Override
    public void close() throws IOException {
        this.blockStore.close();
    }
}
