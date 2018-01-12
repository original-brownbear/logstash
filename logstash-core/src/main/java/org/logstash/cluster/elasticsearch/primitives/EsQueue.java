package org.logstash.cluster.elasticsearch.primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.logstash.cluster.WorkerTask;
import org.logstash.cluster.elasticsearch.LsEsRestClient;
import org.logstash.cluster.execution.LeaderElectionAction;
import org.logstash.cluster.state.Partition;
import org.logstash.cluster.state.Task;

public final class EsQueue {

    private static final AtomicInteger HASH_SOURCE = new AtomicInteger(0);

    private final LsEsRestClient client;

    public static EsQueue create(final LsEsRestClient esClient) {
        return new EsQueue(esClient);
    }

    private EsQueue(final LsEsRestClient esClient) {
        client = esClient;
    }

    public boolean pushTask(final WorkerTask task) {
        final List<Partition> partitions = new ArrayList<>(
            Partition.fromMap(EsMap.create(client, LeaderElectionAction.PARTITION_MAP_DOC))
        );
        final int partitionCount = partitions.size();
        if (partitionCount > 0) {
            partitions.get(HASH_SOURCE.incrementAndGet() % partitionCount).pushTask(task);
            return true;
        } else {
            return false;
        }
    }

    public Task nextTask() {
        final Optional<Task> taskOptional = Partition.fromMap(
            EsMap.create(client, LeaderElectionAction.PARTITION_MAP_DOC)
        ).stream().filter(partition -> partition.getOwner().equals(client.getConfig().localNode()))
            .map(Partition::getCurrentTask).filter(Objects::nonNull).findFirst();
        return taskOptional.orElse(null);
    }

}
