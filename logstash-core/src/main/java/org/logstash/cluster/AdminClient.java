package org.logstash.cluster;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.logstash.ObjectMappers;

public final class AdminClient {

    private static final Logger LOGGER = LogManager.getLogger(AdminClient.class);

    public static void main(final String... args) throws IOException, ExecutionException, InterruptedException {
        final InetAddress host = InetAddress.getByName(args[0]);
        final int port = Integer.parseInt(args[1]);
        final String index = args[2];
        final String jsonConfig = args[3];
        try (
            final TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(host, port));
            final ClusterConfigProvider esClient = ClusterConfigProvider.esConfigProvider(
                transportClient, new LogstashClusterConfig(index)
            )
        ) {
            LOGGER.info("Configuring {}", jsonConfig);
            esClient.publishJobSettings(
                ObjectMappers.JSON_MAPPER.readerFor(Map.class).readValue(jsonConfig)
            );
        }
    }
}
