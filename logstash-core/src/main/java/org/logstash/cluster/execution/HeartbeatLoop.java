package org.logstash.cluster.execution;

import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.elasticsearch.EsClient;

public final class HeartbeatLoop implements Runnable {

    private static final Logger LOGGER =
        LogManager.getLogger(HeartbeatLoop.class);

    private final EsClient client;

    public HeartbeatLoop(final EsClient client) {
        this.client = client;
    }

    @Override
    public void run() {
        final String local = client.getConfig().localNode();
        LOGGER.info("Started background heartbeat loop on {}", local);
        while (!Thread.currentThread().isInterrupted()) {
            if (!client.currentClusterNodes().contains(local)) {
                LOGGER.info(
                    "Publishing local node {} since it wasn't found in the node list.",
                    local
                );
                client.publishLocalNode();
                LOGGER.info("Published local node {} to node list.", local);
            }
            try {
                TimeUnit.SECONDS.sleep(5L);
            } catch (final InterruptedException ex) {
                LOGGER.error("Background heartbeat loop on {} interrupted", local);
                throw new IllegalStateException(ex);
            }
        }
    }
}
