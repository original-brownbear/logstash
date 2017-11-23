package org.logstash.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.logstash.ObjectMappers;

final class EsBlockstore implements BlockStore {

    private final HttpHost[] esHosts;

    private transient RestClient esClient;

    EsBlockstore(final HttpHost... esHosts) {
        this.esHosts = esHosts.clone();
        esClient = RestClient.builder(this.esHosts).build();
    }

    @Override
    public void store(final BlockId key, final ByteBuffer buffer) throws IOException {
        connect();
        final byte[] raw = new byte[buffer.remaining()];
        buffer.get(raw);
        final String rawData = Base64.getEncoder().encodeToString(raw);
        final Map<String, Object> script = new HashMap<>();
        script.put(
            "source",
            "ctx._source.put(params.key, params.value)"
        );
        script.put("lang", "painless");
        final Map<String, Object> params = new HashMap<>();
        script.put("params", params);
        params.put("value", rawData);
        params.put("key", key.identifier);
        final Map<String, Object> data = new HashMap<>();
        data.put("script", script);
        final Map<String, String> upsert = new HashMap<>();
        upsert.put(key.identifier, rawData);
        data.put("upsert", upsert);
        final String json;
        try {
            json = ObjectMappers.JSON_MAPPER.writeValueAsString(data);
        } catch (final JsonProcessingException ex) {
            throw new IllegalStateException(ex);
        }
        final int result = esClient.performRequest(
            "POST", String.format("/logstash-cluster-%s/%s/1/_update", key.clusterName, key.group),
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
    public Void load(final BlockId key, final ByteBuffer buffer) throws IOException {
        connect();
        final Response result = esClient.performRequest(
            "GET", String.format("/logstash-cluster-%s/%s/1", key.clusterName, key.group),
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
        final Map<String, Object> data = ObjectMappers.JSON_MAPPER.readerFor(Map.class)
            .readValue(result.getEntity().getContent());
        final String value = ((Map<String, String>) data.get("_source")).get(key.identifier);
        buffer.put(Base64.getDecoder().decode(value));
        return null;
    }

    @Override
    public void delete(final BlockId key) throws IOException {
        connect();
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<BlockId> group(final BlockId key) throws IOException {
        connect();
        final Response result = esClient.performRequest(
            "GET", String.format("/logstash-cluster-%s/%s/1", key.clusterName, key.group),
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
        final Map<String, Object> data = ObjectMappers.JSON_MAPPER.readerFor(Map.class)
            .readValue(result.getEntity().getContent());
        final Collection<String> value = ((Map<String, String>) data.get("_source")).keySet();
        return value.stream().map(ident -> new BlockId(key.clusterName, key.group, ident)).iterator();
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
