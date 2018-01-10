package org.logstash.cluster.elasticsearch;

public final class EsLock {

    private final String name;

    private final LsEsRestClient client;

    public static EsLock create(final LsEsRestClient esClient, final String name) {
        return new EsLock(esClient, name);
    }

    private EsLock(final LsEsRestClient esClient, final String name) {
        client = esClient;
        this.name = name;
    }

    public boolean lock(final long expire) {
        return false;
    }

    public void unlock() {

    }
}
