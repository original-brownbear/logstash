package org.logstash.cluster.execution;

import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.ClusterInput;
import org.logstash.cluster.elasticsearch.EsClient;
import org.logstash.cluster.elasticsearch.primitives.EsLock;

public final class LeaderElectionLoop implements Runnable {

    private static final Logger LOGGER =
        LogManager.getLogger(LeaderElectionLoop.class);

    private final EsClient client;

    public LeaderElectionLoop(final EsClient client) {
        this.client = client;
    }

    @Override
    public void run() {
        final String local = client.getConfig().localNode();
        LOGGER.info("Started background leader election loop on {}", local);
        final EsLock leaderLock = client.lock(ClusterInput.LEADERSHIP_IDENTIFIER);
        try {
            while (!Thread.currentThread().isInterrupted()) {
                final long expire = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30L);
                LOGGER.info("Trying to acquire leader lock until {} on {}", expire, local);
                if (leaderLock.lock(expire)) {
                    LOGGER.info("{} acquired leadership until {}", local, expire);
                    TimeUnit.MILLISECONDS.sleep(
                        expire - System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10L)
                    );
                } else {
                    final EsLock.LockState lockState = leaderLock.holder();
                    LOGGER.info(
                        "{} did not acquire leadership since {} acquired leadership until {}",
                        local, lockState.getHolder(), lockState.getExpire());
                    TimeUnit.MILLISECONDS.sleep(
                        lockState.getExpire() - System.currentTimeMillis()
                    );
                }
            }
        } catch (final Exception ex) {
            LOGGER.error("Error in leader election loop:", ex);
            throw new IllegalStateException(ex);
        } finally {
            LOGGER.info("Background leader election loop stopped on {}", local);
            leaderLock.unlock();
        }
    }
}
