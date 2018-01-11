package org.logstash.cluster.execution;

import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.ClusterInput;
import org.logstash.cluster.elasticsearch.EsClient;
import org.logstash.cluster.elasticsearch.primitives.EsLock;

public final class LeaderElectionAction implements Runnable {

    public static final long TERM_LENGTH = TimeUnit.SECONDS.toMillis(30L);

    public static final long ELECTION_PERIOD = TERM_LENGTH / 2L;

    private static final Logger LOGGER = LogManager.getLogger(LeaderElectionAction.class);

    private final EsClient client;

    public LeaderElectionAction(final EsClient client) {
        this.client = client;
    }

    @Override
    public void run() {
        final String local = client.getConfig().localNode();
        LOGGER.info("Started background leader election loop on {}", local);
        final EsLock leaderLock = client.lock(ClusterInput.LEADERSHIP_IDENTIFIER);
        try {
            final long expire = System.currentTimeMillis() + TERM_LENGTH;
            LOGGER.info("Trying to acquire leader lock until {} on {}", expire, local);
            if (leaderLock.lock(expire)) {
                LOGGER.info("{} acquired leadership until {}", local, expire);
            } else {
                final EsLock.LockState lockState = leaderLock.holder();
                LOGGER.info(
                    "{} did not acquire leadership since {} acquired leadership until {}",
                    local, lockState.getHolder(), lockState.getExpire());
            }
        } catch (final Exception ex) {
            LOGGER.error("Error in leader election loop:", ex);
            throw new IllegalStateException(ex);
        }
    }
}
