package org.logstash.cluster;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.ObjectMappers;
import org.logstash.cluster.elasticsearch.EsClient;

public final class AdminClient {

    private static final Logger LOGGER = LogManager.getLogger(AdminClient.class);

    public static void main(final String... args) throws IOException {
        final InetAddress host = InetAddress.getByName(args[0]);
        final int port = Integer.parseInt(args[1]);
        final String index = args[2];
        final String jsonConfig = args[3];
        try (final EsClient esClient =
                 EsClient.create(
                     new LogstashClusterConfig(
                         index, Collections.singletonList(new HttpHost(host, port))
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
