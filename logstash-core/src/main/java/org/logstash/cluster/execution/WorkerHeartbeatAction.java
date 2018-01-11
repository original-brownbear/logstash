package org.logstash.cluster.execution;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.elasticsearch.EsClient;
import org.logstash.cluster.state.Partition;

public final class WorkerHeartbeatAction implements Runnable {

    public static final long HEARTBEAT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5L);

    public static final long PARTITION_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(15L);

    private static final Logger LOGGER =
        LogManager.getLogger(WorkerHeartbeatAction.class);

    private final EsClient client;

    private final String local;

    public WorkerHeartbeatAction(final EsClient client) {
        this.client = client;
        local = client.getConfig().localNode();
        LOGGER.info("Started background heartbeat loop on {}", local);
    }

    @Override
    public void run() {
        publishOwnNode();
        maintainPartitionAssignments();
    }

    private void publishOwnNode() {
        if (!client.currentClusterNodes().contains(local)) {
            LOGGER.info(
                "Publishing local node {} since it wasn't found in the node list.",
                local
            );
            client.publishLocalNode();
            LOGGER.info("Published local node {} to node list.", local);
        }
    }

    private void maintainPartitionAssignments() {
        final Collection<Partition> partitions = client.getPartitions();
        final Collection<Partition> ownPartitions = partitions.stream()
            .filter(partition -> partition.getOwner().equals(local))
            .collect(Collectors.toList());
        final int ownCount = ownPartitions.size();
        if (ownCount > 0) {
            LOGGER.info("Refreshing lock on {} partitions on {}", ownCount, local);
        }
        final Collection<Partition> unassigned = partitions.stream()
            .filter(partition ->
                partition.getOwner().isEmpty() || partition.getExpire() < System.currentTimeMillis()
            ).collect(Collectors.toList());
        final int unassignedCount = unassigned.size();
        if (unassignedCount > 0 && ownCount < LeaderElectionAction.PARTITIONS_PER_NODE) {
            LOGGER.info("Found {} unassigned partitions on {}", unassignedCount, local);
            final Collection<Partition> aquired = partitions.stream()
                .limit((long) Math.min(unassignedCount, ownCount - LeaderElectionAction.PARTITIONS_PER_NODE)).filter(
                    Partition::acquire
                ).collect(Collectors.toList());
        }
    }
}
