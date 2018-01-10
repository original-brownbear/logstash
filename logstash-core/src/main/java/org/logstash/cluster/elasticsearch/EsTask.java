package org.logstash.cluster.elasticsearch;

import org.logstash.cluster.EnqueueEvent;

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
