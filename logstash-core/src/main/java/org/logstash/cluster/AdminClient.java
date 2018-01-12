package org.logstash.cluster;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.ObjectMappers;
import org.logstash.cluster.elasticsearch.EsClient;

public final class AdminClient {

    private static final Logger LOGGER = LogManager.getLogger(AdminClient.class);

    private AdminClient() {
        // Entry Point
    }

    public static void main(final String... args) throws IOException {
        final String esHttp = args[0];
        final String index = args[1];
        final String jsonConfig = args[2];
        try (final EsClient esClient =
                 EsClient.create(
                     new LogstashClusterConfig(
                         index, Collections.singletonList(HttpHost.create(esHttp))
                     )
                 )
        ) {
            LOGGER.info("Configuring {}", jsonConfig);
            esClient.publishJobSettings(
                ObjectMappers.JSON_MAPPER.readerFor(Map.class).readValue(jsonConfig)
            );
        }
    }
}
