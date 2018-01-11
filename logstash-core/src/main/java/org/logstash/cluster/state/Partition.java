package org.logstash.cluster.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.logstash.cluster.elasticsearch.primitives.EsLock;
import org.logstash.cluster.elasticsearch.primitives.EsMap;
import org.logstash.cluster.execution.WorkerHeartbeatAction;

public final class Partition {

    private final EsMap map;

    private final int id;

    @SuppressWarnings("unchecked")
    public static Collection<Partition> fromMap(final EsMap table) {
        final Map<String, Object> raw = table.asMap();
        final Collection<Partition> partitions = new ArrayList<>();
        raw.forEach((key, value) -> {
            if (key.length() > 1 && key.charAt(0) == 'p') {
                partitions.add(new Partition(table, Integer.parseInt(key.substring(1))));
            }
        });
        return partitions;
    }

    private Partition(final EsMap map, final int id) {
        this.map = map;
        this.id = id;
    }

    @SuppressWarnings("unchecked")
    public boolean acquire() {
        final Map<String, Object> updated = new HashMap<>();
        final String local = map.getClient().getConfig().localNode();
        updated.put(EsLock.TOKEN_KEY, local);
        updated.put(
            EsLock.EXPIRE_TIME_KEY,
            System.currentTimeMillis() + WorkerHeartbeatAction.PARTITION_TIMEOUT_MS
        );
        return map.putAllConditionally(
            Collections.singletonMap(String.format("p%d", id), updated), current -> {
                final Map<String, Object> raw =
                    (Map<String, Object>) current.get(String.format("p%d", id));
                return (long) raw.get(EsLock.EXPIRE_TIME_KEY) < System.currentTimeMillis()
                    || raw.get(EsLock.TOKEN_KEY).equals(local);
            }
        );
    }

    @SuppressWarnings("unchecked")
    public String getOwner() {
        return (String) ((Map<String, Object>) map.asMap()
            .get(String.format("p%d", id))).get(EsLock.TOKEN_KEY);
    }

    @SuppressWarnings("unchecked")
    public long getExpire() {
        return (long) ((Map<String, Object>) map.asMap()
            .get(String.format("p%d", id))).get(EsLock.EXPIRE_TIME_KEY);
    }

    public int getId() {
        return id;
    }
}
