package org.logstash.cluster.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.WorkerTask;
import org.logstash.cluster.elasticsearch.primitives.EsLock;
import org.logstash.cluster.elasticsearch.primitives.EsMap;
import org.logstash.cluster.execution.TimingConstants;
import org.logstash.cluster.io.TaskSerializer;

public final class Partition {

    private static final Logger LOGGER = LogManager.getLogger(Partition.class);

    private final EsMap lockMap;

    private final EsMap tasksMap;

    private final int id;

    private final String mapKey;

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
        this.lockMap = map;
        this.id = id;
        mapKey = String.format("p%d", id);
        tasksMap = EsMap.create(map.getClient(), String.format("tasksp%d", id));
    }

    public void pushTask(final WorkerTask task) {
        LOGGER.info("Pushing new task to partition {}", id);
        final Map<String, Object> existing = tasksMap.asMap();
        final int newId = existing.size();
        final Map<String, Object> taskMap = new HashMap<>();
        taskMap.put(Task.PAYLOAD_FIELD, TaskSerializer.serialize(task));
        taskMap.put(Task.CREATED_FIELD_KEY, System.currentTimeMillis());
        taskMap.put(Task.STATE_FIELD_KEY, Task.State.OUTSTANDING);
        if (tasksMap.putAllConditionally(
            Collections.singletonMap(String.format("t%d", newId), taskMap),
            current -> current == null || !current.containsKey(String.format("t%d", newId))
        )) {
            LOGGER.info("Pushed new task to partition {}", id);
        } else {
            LOGGER.warn("Failed to push new task to partition {} due to contention", id);
        }
    }

    public Task getCurrentTask() {
        return Task.fromMap(tasksMap).stream().filter(t -> t.getState() == Task.State.OUTSTANDING)
            .min(Comparator.comparingLong(Task::getCreated)).orElse(null);
    }

    @SuppressWarnings("unchecked")
    public boolean acquire() {
        final Map<String, Object> updated = new HashMap<>();
        final String local = lockMap.getClient().getConfig().localNode();
        updated.put(EsLock.TOKEN_KEY, local);
        updated.put(
            EsLock.EXPIRE_TIME_KEY,
            System.currentTimeMillis() + TimingConstants.PARTITION_TIMEOUT_MS
        );
        return lockMap.putAllConditionally(
            Collections.singletonMap(mapKey, updated), current -> {
                final Map<String, Object> raw = (Map<String, Object>) current.get(mapKey);
                return (long) raw.get(EsLock.EXPIRE_TIME_KEY) < System.currentTimeMillis()
                    || raw.get(EsLock.TOKEN_KEY).equals(local);
            }
        );
    }

    @SuppressWarnings("unchecked")
    public String getOwner() {
        return (String) ((Map<String, Object>) lockMap.asMap().get(mapKey)).get(EsLock.TOKEN_KEY);
    }

    @SuppressWarnings("unchecked")
    public long getExpire() {
        return (long) ((Map<String, Object>) lockMap.asMap()
            .get(mapKey)).get(EsLock.EXPIRE_TIME_KEY);
    }

    public int getId() {
        return id;
    }
}
