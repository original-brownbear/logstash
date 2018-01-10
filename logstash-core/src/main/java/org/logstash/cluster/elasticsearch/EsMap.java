package org.logstash.cluster.elasticsearch;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.ResponseException;
import org.logstash.ObjectMappers;

public final class EsMap {

    private final String name;

    private final LsEsRestClient client;

    public static EsMap create(final LsEsRestClient esClient, final String name) {
        return new EsMap(esClient, name);
    }

    private EsMap(final LsEsRestClient esClient, final String name) {
        client = esClient;
        this.name = name;
    }

    public boolean containsKey(final String key) {
        try {
            final GetResponse response = client.getClient().get(
                new GetRequest().index(client.getConfig().esIndex()).id(name)
            );
            return response.isExists() && response.getSource().containsKey(key);
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void put(final String key, final Object value) {
        putAll(Collections.singletonMap(key, value));
    }

    public void putAll(final Map<String, Object> entries) {
        try {
            final GetResponse response = client.getClient().get(
                new GetRequest().index(client.getConfig().esIndex()).id(name)
            );
            if (response.isExists()) {
                final long version = response.getVersion();
                final Map<String, Object> data = response.getSource();
                final Map<String, Object> updated = new HashMap<>();
                updated.putAll(data);
                updated.putAll(entries);
                client.getClient().getLowLevelClient().performRequest(
                    "PUT", String.format("/%s/maps/%s", client.getConfig().esIndex(), name),
                    Collections.singletonMap("version", String.valueOf(version)),
                    new NStringEntity(
                        ObjectMappers.JSON_MAPPER.writer().forType(Map.class)
                            .writeValueAsString(updated)
                    ),
                    new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                );
            } else {
                client.getClient().getLowLevelClient().performRequest(
                    "PUT", String.format("/%s/maps/%s/_create", client.getConfig().esIndex(), name),
                    Collections.emptyMap(),
                    new NStringEntity(
                        ObjectMappers.JSON_MAPPER.writer().forType(Map.class)
                            .writeValueAsString(entries)
                    ),
                    new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                );
            }
        } catch (final ResponseException ex) {
            if (ex.getResponse().getStatusLine().getStatusCode() == 409) {
                putAll(entries);
            }
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void remove(final String key) {

    }

    public Map<String, Object> asMap() {
        try {
            final GetResponse response = client.getClient().get(
                new GetRequest().index(client.getConfig().esIndex()).id(name)
            );
            if (response.isExists()) {
                return response.getSource();
            } else {
                return Collections.emptyMap();
            }
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
