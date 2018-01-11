package org.logstash.cluster.elasticsearch.primitives;

import java.util.ArrayList;
import java.util.List;
import org.logstash.cluster.WorkerTask;
import org.logstash.cluster.elasticsearch.LsEsRestClient;
import org.logstash.cluster.execution.LeaderElectionAction;
import org.logstash.cluster.state.Partition;

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

    public void pushTask(final WorkerTask task) {
        final List<Partition> partitions = new ArrayList<>(
            Partition.fromMap(EsMap.create(client, LeaderElectionAction.PARTITION_MAP_DOC))
        );
        partitions.get(task.hashCode() % partitions.size()).pushTask(task);
    }

    public void complete(final WorkerTask task) {

    }

    public WorkerTask nextTask() {
        return null;
    }

}
