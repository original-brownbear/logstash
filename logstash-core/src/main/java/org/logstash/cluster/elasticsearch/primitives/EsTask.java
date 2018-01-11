package org.logstash.cluster.elasticsearch.primitives;

import org.logstash.cluster.EnqueueEvent;
import org.logstash.cluster.elasticsearch.LsEsRestClient;

public final class EsTask {

    private final LsEsRestClient client;

    private final String name;

    public EsTask(final LsEsRestClient client, final String name) {
        this.client = client;
        this.name = name;
    }

    public EnqueueEvent payload() {
        return null;
    }

    public long heartbeat() {
        return 0L;
    }

    public void complete() {

    }

    public void relinquish() {

    }
}
