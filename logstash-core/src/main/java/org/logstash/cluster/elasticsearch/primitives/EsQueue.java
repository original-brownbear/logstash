package org.logstash.cluster.elasticsearch.primitives;

import java.io.Closeable;
import java.io.IOException;
import org.logstash.cluster.EnqueueEvent;
import org.logstash.cluster.elasticsearch.LsEsRestClient;

public final class EsQueue implements Closeable {

    private final String name;

    private final LsEsRestClient client;

    public static EsQueue create(final LsEsRestClient esClient, final String name) {
        return new EsQueue(esClient, name);
    }

    private EsQueue(final LsEsRestClient esClient, final String name) {
        client = esClient;
        this.name = name;
    }

    public void pushTask(final EnqueueEvent task) {

    }

    public void complete(final EnqueueEvent task) {

    }

    public EnqueueEvent nextTask() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
