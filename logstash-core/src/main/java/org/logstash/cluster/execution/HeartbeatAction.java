package org.logstash.cluster.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.elasticsearch.EsClient;

public final class HeartbeatAction implements Runnable {

    private static final Logger LOGGER =
        LogManager.getLogger(HeartbeatAction.class);

    private final EsClient client;

    private final String local;

    public HeartbeatAction(final EsClient client) {
        this.client = client;
        local = client.getConfig().localNode();
        LOGGER.info("Started background heartbeat loop on {}", local);
    }

    @Override
    public void run() {
        if (!client.currentClusterNodes().contains(local)) {
            LOGGER.info(
                "Publishing local node {} since it wasn't found in the node list.",
                local
            );
            client.publishLocalNode();
            LOGGER.info("Published local node {} to node list.", local);
        }
    }
}
