package org.logstash.cluster.execution;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.ClusterInput;
import org.logstash.cluster.elasticsearch.primitives.EsLock;
import org.logstash.cluster.elasticsearch.primitives.EsMap;
import org.logstash.cluster.state.Partition;

public final class LeaderElectionAction implements Runnable, AutoCloseable {

    public static final String PARTITION_MAP_DOC = "partition-map";

    public static final int PARTITIONS_PER_NODE = 1;

    public static final long TERM_LENGTH = TimeUnit.SECONDS.toMillis(30L);

    public static final long ELECTION_PERIOD = TERM_LENGTH / 10L;

    private static final Logger LOGGER = LogManager.getLogger(LeaderElectionAction.class);

    private final ScheduledExecutorService leaderExecutor =
        Executors.newSingleThreadScheduledExecutor();

    private final ClusterInput input;

    private final EsLock leaderLock;

    private final EsMap partitionMap;

    private final String local;

    private final AtomicReference<ScheduledFuture<?>> leaderTask = new AtomicReference<>();

    public LeaderElectionAction(final ClusterInput input) {
        this.input = input;
        this.leaderLock = input.getEsClient().lock(ClusterInput.LEADERSHIP_IDENTIFIER);
        local = input.getEsClient().getConfig().localNode();
        partitionMap = input.getEsClient().map(PARTITION_MAP_DOC);
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
                        leaderTask.set(
                            leaderExecutor.scheduleAtFixedRate(
                                () -> {
                                    LOGGER.info("Execution leader action on {}", local);
                                    try {
                                        leaderAction.run();
                                    } catch (final Exception ex) {
                                        LOGGER.error("Leader action failed because of: ", ex);
                                    }
                                }, 0L, 1L, TimeUnit.SECONDS
                            )
                        );
                    }
                }
                maintainPartitions();
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

    @Override
    public void close() {
        leaderExecutor.shutdownNow();
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
                .newInstance(this.input);
        } catch (final Exception ex) {
            LOGGER.error("Failed to set up leader task because of:", ex);
            throw new IllegalStateException(ex);
        }
    }

    private void maintainPartitions() {
        LOGGER.info("Starting partition table maintenance.");
        final Map<String, Object> current = partitionMap.asMap();
        final Collection<String> clusterNodes = input.getEsClient().currentClusterNodes();
        if (current == null || current.isEmpty()) {
            LOGGER.info("No partition table found, creating it.");
            final int partitions = PARTITIONS_PER_NODE * clusterNodes.size();
            final Map<String, Object> newMap = new HashMap<>();
            IntStream.range(0, partitions).forEach(
                partition -> {
                    final Map<String, Object> partitionEntry = new HashMap<>();
                    partitionEntry.put(EsLock.TOKEN_KEY, "");
                    partitionEntry.put(EsLock.EXPIRE_TIME_KEY, System.currentTimeMillis());
                    newMap.put(String.format("p%d", partition), partitionEntry);
                }
            );
            if (partitionMap.putAllConditionally(
                newMap, existing -> existing == null || existing.isEmpty()
            )) {
                LOGGER.info("Partition table created. ({} Partitions)", partitions);
            }
        } else {
            LOGGER.info("Partition table found, performing maintenance cycle.");
            final Collection<Partition> partitions = input.getEsClient().getPartitions();
            final int pCount = partitions.size();
            final int nCount = clusterNodes.size();
            final int outstanding = (nCount - pCount) * PARTITIONS_PER_NODE;
            if (outstanding > 0) {
                LOGGER.info(
                    "Found {} partitions and {} nodes, creating {} new partitions.",
                    pCount, nCount, outstanding
                );
                final Map<String, Object> newMap = new HashMap<>();
                IntStream.range(pCount, pCount + outstanding).forEach(
                    partition -> {
                        final Map<String, Object> partitionEntry = new HashMap<>();
                        partitionEntry.put(EsLock.TOKEN_KEY, "");
                        partitionEntry.put(EsLock.EXPIRE_TIME_KEY, System.currentTimeMillis());
                        newMap.put(String.format("p%d", partition), partitionEntry);
                    }
                );
                if (partitionMap.putAllConditionally(
                    newMap, existing -> !existing.containsKey(String.format("p%d", pCount))
                )) {
                    LOGGER.info(
                        "Partition table updated. ({} new partitions created)", outstanding
                    );
                } else {
                    LOGGER.warn(
                        "Failed to updated partition table due to lock contention, retrying in next election cycle."
                    );
                }
            }
        }
    }
}
