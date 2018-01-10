package org.logstash.cluster.elasticsearch;

import java.io.Closeable;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.EnqueueEvent;

public final class EsQueue implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(EsQueue.class);

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
