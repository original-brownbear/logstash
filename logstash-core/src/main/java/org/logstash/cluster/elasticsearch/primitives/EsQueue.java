package org.logstash.cluster.elasticsearch.primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.logstash.cluster.WorkerTask;
import org.logstash.cluster.elasticsearch.LsEsRestClient;
import org.logstash.cluster.execution.LeaderElectionAction;
import org.logstash.cluster.state.Partition;
import org.logstash.cluster.state.Task;

public final class EsQueue {

    private final String name;

    private final LsEsRestClient client;

    public static EsQueue create(final LsEsRestClient esClient, final String name) {
        return new EsQueue(esClient, name);
    }

    private EsQueue(final LsEsRestClient esClient, final String name) {
        client = esClient;
        this.name = name;
    }

    public boolean pushTask(final WorkerTask task) {
        final List<Partition> partitions = new ArrayList<>(
            Partition.fromMap(EsMap.create(client, LeaderElectionAction.PARTITION_MAP_DOC))
        );
        final int partitionCount = partitions.size();
        if (partitionCount > 0) {
            partitions.get(task.hashCode() % partitionCount).pushTask(task);
            return true;
        } else {
            return false;
        }
    }

    public void complete(final WorkerTask task) {

    }

    public WorkerTask nextTask() {
        final Optional<Task> taskOptional = Partition.fromMap(
            EsMap.create(client, LeaderElectionAction.PARTITION_MAP_DOC)
        ).stream().filter(partition -> partition.getOwner().equals(client.getConfig().localNode()))
            .map(Partition::getCurrentTask).filter(Objects::nonNull).findFirst();
        if (taskOptional.isPresent()) {
            return taskOptional.get().getTask();
        } else {
            return null;
        }
    }

}
