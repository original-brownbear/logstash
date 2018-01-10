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

    public EsLock.LockState holder() {
        return null;
    }

    public boolean lock(final long expire) {

        return false;
    }

    public void unlock() {

    }

    public static final class LockState {

        private final String holder;

        private final long expire;

        LockState(final String holder, final long expire) {
            this.holder = holder;
            this.expire = expire;
        }

        public String getHolder() {
            return holder;
        }

        public long getExpire() {
            return expire;
        }
    }
}
