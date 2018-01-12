package org.logstash.cluster.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.logstash.cluster.WorkerTask;
import org.logstash.cluster.elasticsearch.primitives.EsMap;
import org.logstash.cluster.io.TaskSerializer;

public final class Task {

    public static final String STATE_FIELD_KEY = "state";

    public static final String CREATED_FIELD_KEY = "created";

    public static final String PAYLOAD_FIELD = "payload";

    private final int id;

    private final Partition partition;

    private final EsMap map;

    public static Collection<Task> fromMap(final EsMap table, final Partition partition) {
        final Map<String, Object> raw = table.asMap();
        final Collection<Task> tasks = new ArrayList<>();
        raw.forEach((key, value) -> {
            if (key.length() > 1 && key.charAt(0) == 't') {
                tasks.add(new Task(table, Integer.parseInt(key.substring(1)), partition));
            }
        });
        return tasks;
    }

    private Task(final EsMap map, final int id, final Partition partition) {
        this.map = map;
        this.id = id;
        this.partition = partition;
    }

    public Partition getPartition() {
        return partition;
    }

    public int getId() {
        return id;
    }

    @SuppressWarnings("unchecked")
    public WorkerTask getTask() {
        return TaskSerializer.deserialize((String) getTaskData().get(PAYLOAD_FIELD));
    }

    @SuppressWarnings("unchecked")
    public long getCreated() {
        return (long) getTaskData().get(CREATED_FIELD_KEY);
    }

    @SuppressWarnings("unchecked")
    public Task.State getState() {
        return Task.State.valueOf((String) getTaskData().get(STATE_FIELD_KEY));
    }

    @SuppressWarnings("unchecked")
    public void complete() {
        final String taskKey = String.format("t%d", id);
        final Map<String, Object> current = (Map<String, Object>) map.asMap().get(taskKey);
        final Map<String, Object> updated = new HashMap<>(current);
        updated.put(STATE_FIELD_KEY, Task.State.COMPLETE);
        map.put(taskKey, updated);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getTaskData() {
        return (Map<String, Object>) map.asMap().get(String.format("t%d", id));
    }

    public enum State {
        COMPLETE, OUTSTANDING
    }

}
