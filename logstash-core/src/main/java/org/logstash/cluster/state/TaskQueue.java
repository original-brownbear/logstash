package org.logstash.cluster.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.logstash.cluster.WorkerTask;
import org.logstash.cluster.elasticsearch.LsEsRestClient;
import org.logstash.cluster.elasticsearch.primitives.EsMap;
import org.logstash.cluster.execution.LsClusterDocuments;

public final class TaskQueue {

    private static final AtomicInteger HASH_SOURCE = new AtomicInteger(0);

    private final LsEsRestClient client;

    public static TaskQueue create(final LsEsRestClient esClient) {
        return new TaskQueue(esClient);
    }

    private TaskQueue(final LsEsRestClient esClient) {
        client = esClient;
    }

    public boolean pushTask(final WorkerTask task) {
        final List<Partition> partitions = getPartitions();
        final int partitionCount = partitions.size();
        if (partitionCount > 0) {
            partitions.get(HASH_SOURCE.incrementAndGet() % partitionCount).pushTask(task);
            return true;
        } else {
            return false;
        }
    }

    public Task nextTask() {
        final Optional<Task> taskOptional = getPartitions().stream()
            .filter(partition -> partition.getOwner().equals(client.getConfig().localNode()))
            .map(Partition::getCurrentTask).filter(Objects::nonNull).findFirst();
        return taskOptional.orElse(null);
    }

    private List<Partition> getPartitions() {
        return new ArrayList<>(
            Partition.fromMap(EsMap.create(client, LsClusterDocuments.PARTITION_MAP_DOC))
        );
    }

}
