package org.logstash.cluster.elasticsearch;

import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;
import org.logstash.cluster.LogstashClusterConfig;

public final class LsEsRestClient implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(LsEsRestClient.class);

    private final LogstashClusterConfig config;

    private final RestHighLevelClient client;

    public LsEsRestClient(final LogstashClusterConfig config, final RestHighLevelClient client) {
        this.config = config;
        this.client = createIndex(config, client);
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public LogstashClusterConfig getConfig() {
        return config;
    }

    private static RestHighLevelClient createIndex(final LogstashClusterConfig config,
        final RestHighLevelClient client) {
        try {
            try {
                client.getLowLevelClient().performRequest("GET", config.esIndex());
            } catch (final ResponseException ex) {
                if (ex.getResponse().getStatusLine().getStatusCode() ==
                    HttpURLConnection.HTTP_NOT_FOUND) {
                    client.getLowLevelClient().performRequest("PUT", config.esIndex());
                }
            }
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
        return client;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
