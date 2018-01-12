package org.logstash.cluster.elasticsearch.primitives;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.ResponseException;
import org.logstash.ObjectMappers;
import org.logstash.cluster.elasticsearch.LsEsRestClient;

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

    public LsEsRestClient getClient() {
        return client;
    }

    public boolean containsKey(final String key) {
        try {
            final GetResponse response = client.getClient().get(
                new GetRequest().index(client.getConfig().esIndex()).type("maps").id(name)
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
                executePut(
                    updated,
                    String.format("/%s/maps/%s", client.getConfig().esIndex(), name),
                    Collections.singletonMap("version", String.valueOf(version))
                );
            } else {
                executePut(
                    entries,
                    String.format("/%s/maps/%s/_create", client.getConfig().esIndex(), name),
                    Collections.emptyMap()
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

    public boolean putAllConditionally(final Map<String, Object> entries,
        final Predicate<? super Map<String, Object>> predicate) {
        try {
            final GetResponse response = client.getClient().get(
                new GetRequest().index(client.getConfig().esIndex()).type("maps").id(name)
            );
            final long version = response.getVersion();
            final Map<String, Object> source = response.getSource();
            if (predicate.test(source)) {
                if (source != null) {
                    final Map<String, Object> updated = new HashMap<>();
                    updated.putAll(source);
                    updated.putAll(entries);
                    executePut(
                        updated,
                        String.format("/%s/maps/%s", client.getConfig().esIndex(), name),
                        Collections.singletonMap("version", String.valueOf(version))
                    );
                } else {
                    executePut(
                        entries,
                        String.format("/%s/maps/%s/_create", client.getConfig().esIndex(), name),
                        Collections.emptyMap()
                    );
                }
                return true;
            } else {
                return false;
            }
        } catch (final ResponseException ex) {
            if (ex.getResponse().getStatusLine().getStatusCode() == 409) {
                return false;
            } else {
                throw new IllegalStateException(ex);
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

    private void executePut(final Map<String, Object> entries, final String path,
        final Map<String, String> params) throws IOException {
        client.getClient().getLowLevelClient().performRequest(
            "PUT", String.format(path, client.getConfig().esIndex(), name),
            params,
            new NStringEntity(
                ObjectMappers.JSON_MAPPER.writer().forType(Map.class)
                    .writeValueAsString(entries)
            ),
            new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        );
    }
}