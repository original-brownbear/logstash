package org.logstash.cluster.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.logstash.cluster.WorkerTask;
import org.logstash.cluster.elasticsearch.primitives.EsMap;
import org.logstash.cluster.io.TaskSerializer;

public final class Task {

    public static final String STATE_FIELD_KEY = "state";

    public static final String CREATED_FIELD_KEY = "created";

    public static final String PAYLOAD_FIELD = "payload";

    private final int id;

    private final EsMap map;

    public static Collection<Task> fromMap(final EsMap table) {
        final Map<String, Object> raw = table.asMap();
        final Collection<Task> partitions = new ArrayList<>();
        raw.forEach((key, value) -> {
            if (key.length() > 1 && key.charAt(0) == 't') {
                partitions.add(new Task(table, Integer.parseInt(key.substring(1))));
            }
        });
        return partitions;
    }

    private Task(final EsMap map, final int id) {
        this.map = map;
        this.id = id;
    }

    @SuppressWarnings("unchecked")
    public WorkerTask getTask() {
        return TaskSerializer.deserialize(
            (String) ((Map<String, Object>) map.asMap().get(String.format("t%d", id)))
                .get(PAYLOAD_FIELD)
        );
    }

    @SuppressWarnings("unchecked")
    public long getCreated() {
        return (long) ((Map<String, Object>) map.asMap().get(String.format("t%d", id)))
            .get(CREATED_FIELD_KEY);
    }

    @SuppressWarnings("unchecked")
    public Task.State getState() {
        return
            Task.State.valueOf(
                (String) ((Map<String, Object>) map.asMap().get(String.format("t%d", id)))
                    .get(STATE_FIELD_KEY)
            );
    }

    public enum State {
        COMPLETE, OUTSTANDING
    }

}
