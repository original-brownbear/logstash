package org.logstash.cluster.elasticsearch;

public final class EsLock {

    public EsLock(final LsEsRestClient client, final String name) {

    }

    public boolean lock(final long expire) {
        return false;
    }

    public void unlock() {

    }
}
