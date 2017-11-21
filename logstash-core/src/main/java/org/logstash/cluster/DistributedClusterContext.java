package org.logstash.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.logstash.ObjectMappers;

final class DistributedClusterContext implements Closeable {

    private transient UUID nodeId = UUID.randomUUID();

    private transient RestClient esClient;

    private final String clusterName;

    private final HttpHost[] esHosts;

    DistributedClusterContext(final String clusterName, final HttpHost... esHosts) {
        this.clusterName = clusterName;
        this.esHosts = esHosts;
        esClient = RestClient.builder(this.esHosts).build();
    }

    String getClusterName() {
        return clusterName;
    }

    Iterable<SlaveNode> nodes() throws IOException {
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
    }

    void join(final InetSocketAddress address) throws IOException {
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
        }
    }

    @Override
    public void close() throws IOException {
        if (this.esClient != null) {
            esClient.close();
        }
    }

    private void connect() {
        esClient = RestClient.builder(this.esHosts).build();
    }
}
