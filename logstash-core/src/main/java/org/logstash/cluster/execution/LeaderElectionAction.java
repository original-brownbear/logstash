package org.logstash.cluster.execution;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.ClusterInput;
import org.logstash.cluster.elasticsearch.primitives.EsLock;

public final class LeaderElectionAction implements Runnable {

    public static final long TERM_LENGTH = TimeUnit.SECONDS.toMillis(30L);

    public static final long ELECTION_PERIOD = TERM_LENGTH / 2L;

    private static final Logger LOGGER = LogManager.getLogger(LeaderElectionAction.class);

    private final ClusterInput input;

    private final EsLock leaderLock;

    private final String local;

    private final AtomicReference<ScheduledFuture<?>> leaderTask = new AtomicReference<>();

    public LeaderElectionAction(final ClusterInput input) {
        this.input = input;
        this.leaderLock = input.getEsClient().lock(ClusterInput.LEADERSHIP_IDENTIFIER);
        local = input.getEsClient().getConfig().localNode();
    }

    @Override
    public void run() {
        LOGGER.info("Started background leader election loop on {}", local);
        try {
            final long expire = System.currentTimeMillis() + TERM_LENGTH;
            LOGGER.info("Trying to acquire leader lock until {} on {}", expire, local);
            if (leaderLock.lock(expire)) {
                LOGGER.info("{} acquired leadership until {}", local, expire);
                if (leaderTask.get() == null) {
                    LOGGER.info("No configuration set, trying to learn new input configuration from Elasticsearch.");
                    final Runnable leaderAction = setupLeaderTask();
                    if (leaderAction != null) {
                        LOGGER.info("Input configuration ready, starting leader task.");
                        leaderTask.set(input.getExecutor().scheduleAtFixedRate(leaderAction, 0L, 1L, TimeUnit.NANOSECONDS));
                    }
                }
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

    private Runnable setupLeaderTask() {
        try {
            Map<String, Object> configuration;
            if (!(configuration = input.getConfig()).containsKey(ClusterInput.LOGSTASH_TASK_CLASS_SETTING)) {
                LOGGER.info(
                    "No valid cluster input configuration found, will retry in the next election cycle."
                );
                return null;
            }
            final String clazz = (String) configuration.get(ClusterInput.LOGSTASH_TASK_CLASS_SETTING);
            LOGGER.info(
                "Found valid cluster input configuration, starting leader task of type {}.",
                clazz
            );
            return Class.forName(clazz)
                .asSubclass(Runnable.class).getConstructor(ClusterInput.class)
                .newInstance(this);
        } catch (final Exception ex) {
            LOGGER.error("Failed to set up leader task because of: {}", ex);
            throw new IllegalStateException(ex);
        }
    }
}
