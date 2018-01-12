package org.logstash.cluster.elasticsearch.primitives;

import java.util.HashMap;
import java.util.Map;
import org.logstash.cluster.elasticsearch.LsEsRestClient;

public final class EsLock {

    public static final String TOKEN_KEY = "token";

    public static final String EXPIRE_TIME_KEY = "expire";

    private final EsMap map;

    private final String localNode;

    public static boolean canLock(final String token, final Map<String, Object> lockable) {
        return token.equals(lockable.get(TOKEN_KEY))
            || System.currentTimeMillis() > ((Number) lockable.get(EXPIRE_TIME_KEY)).longValue();
    }

    public static EsLock create(final LsEsRestClient esClient, final String name) {
        return new EsLock(esClient, name);
    }

    private EsLock(final LsEsRestClient esClient, final String name) {
        localNode = esClient.getConfig().localNode();
        this.map = EsMap.create(esClient, name);
    }

    public EsLock.LockState holder() {
        final Map<String, Object> current = map.asMap();
        return new EsLock.LockState(
            (String) current.get(TOKEN_KEY), ((Number) current.get(EXPIRE_TIME_KEY)).longValue()
        );
    }

    public boolean lock(final long expire) {
        final Map<String, Object> updated = new HashMap<>();
        updated.put(EXPIRE_TIME_KEY, expire);
        updated.put(TOKEN_KEY, localNode);
        return map.putAllConditionally(
            updated, current ->
                current == null || !current.containsKey(TOKEN_KEY) || canLock(localNode, current)
        );
    }

    public void unlock() {
        map.put(EXPIRE_TIME_KEY, 0L);
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
