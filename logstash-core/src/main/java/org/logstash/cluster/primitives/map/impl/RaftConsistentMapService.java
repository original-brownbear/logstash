package org.logstash.cluster.primitives.map.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.logstash.cluster.primitives.TransactionId;
import org.logstash.cluster.primitives.TransactionLog;
import org.logstash.cluster.primitives.map.MapEvent;
import org.logstash.cluster.protocols.raft.service.AbstractRaftService;
import org.logstash.cluster.protocols.raft.service.Commit;
import org.logstash.cluster.protocols.raft.service.RaftServiceExecutor;
import org.logstash.cluster.protocols.raft.session.RaftSession;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotReader;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotWriter;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.time.Versioned;

/**
 * State Machine for {@link RaftConsistentMap} resource.
 */
public class RaftConsistentMapService extends AbstractRaftService {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftConsistentMapOperations.NAMESPACE)
        .register(RaftConsistentMapEvents.NAMESPACE)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 100)
        .register(RaftConsistentMapService.TransactionScope.class)
        .register(TransactionLog.class)
        .register(TransactionId.class)
        .register(RaftConsistentMapService.MapEntryValue.class)
        .register(RaftConsistentMapService.MapEntryValue.Type.class)
        .register(new HashMap().keySet().getClass())
        .build());

    protected Map<Long, RaftSession> listeners = new LinkedHashMap<>();
    protected Set<String> preparedKeys = Sets.newHashSet();
    protected Map<TransactionId, RaftConsistentMapService.TransactionScope> activeTransactions = Maps.newHashMap();
    protected long currentVersion;
    private Map<String, RaftConsistentMapService.MapEntryValue> map;

    public RaftConsistentMapService() {
        map = createMap();
    }

    protected Map<String, RaftConsistentMapService.MapEntryValue> createMap() {
        return Maps.newHashMap();
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeObject(Sets.newHashSet(listeners.keySet()), serializer()::encode);
        writer.writeObject(preparedKeys, serializer()::encode);
        writer.writeObject(entries(), serializer()::encode);
        writer.writeObject(activeTransactions, serializer()::encode);
        writer.writeLong(currentVersion);
    }

    protected Map<String, RaftConsistentMapService.MapEntryValue> entries() {
        return map;
    }

    protected Serializer serializer() {
        return SERIALIZER;
    }

    @Override
    public void install(SnapshotReader reader) {
        listeners = new LinkedHashMap<>();
        for (Long sessionId : reader.<Set<Long>>readObject(serializer()::decode)) {
            listeners.put(sessionId, sessions().getSession(sessionId));
        }
        preparedKeys = reader.readObject(serializer()::decode);
        map = reader.readObject(serializer()::decode);
        activeTransactions = reader.readObject(serializer()::decode);
        currentVersion = reader.readLong();
    }

    @Override
    protected void configure(RaftServiceExecutor executor) {
        // Listeners
        executor.register(RaftConsistentMapOperations.ADD_LISTENER, (Commit<Void> c) -> listen(c.session()));
        executor.register(RaftConsistentMapOperations.REMOVE_LISTENER, (Commit<Void> c) -> unlisten(c.session()));
        // Queries
        executor.register(RaftConsistentMapOperations.CONTAINS_KEY, serializer()::decode, this::containsKey, serializer()::encode);
        executor.register(RaftConsistentMapOperations.CONTAINS_VALUE, serializer()::decode, this::containsValue, serializer()::encode);
        executor.register(RaftConsistentMapOperations.ENTRY_SET, (Commit<Void> c) -> entrySet(), serializer()::encode);
        executor.register(RaftConsistentMapOperations.GET, serializer()::decode, this::get, serializer()::encode);
        executor.register(RaftConsistentMapOperations.GET_ALL_PRESENT, serializer()::decode, this::getAllPresent, serializer()::encode);
        executor.register(RaftConsistentMapOperations.GET_OR_DEFAULT, serializer()::decode, this::getOrDefault, serializer()::encode);
        executor.register(RaftConsistentMapOperations.IS_EMPTY, (Commit<Void> c) -> isEmpty(), serializer()::encode);
        executor.register(RaftConsistentMapOperations.KEY_SET, (Commit<Void> c) -> keySet(), serializer()::encode);
        executor.register(RaftConsistentMapOperations.SIZE, (Commit<Void> c) -> size(), serializer()::encode);
        executor.register(RaftConsistentMapOperations.VALUES, (Commit<Void> c) -> values(), serializer()::encode);
        // Commands
        executor.register(RaftConsistentMapOperations.PUT, serializer()::decode, this::put, serializer()::encode);
        executor.register(RaftConsistentMapOperations.PUT_IF_ABSENT, serializer()::decode, this::putIfAbsent, serializer()::encode);
        executor.register(RaftConsistentMapOperations.PUT_AND_GET, serializer()::decode, this::putAndGet, serializer()::encode);
        executor.register(RaftConsistentMapOperations.REMOVE, serializer()::decode, this::remove, serializer()::encode);
        executor.register(RaftConsistentMapOperations.REMOVE_VALUE, serializer()::decode, this::removeValue, serializer()::encode);
        executor.register(RaftConsistentMapOperations.REMOVE_VERSION, serializer()::decode, this::removeVersion, serializer()::encode);
        executor.register(RaftConsistentMapOperations.REPLACE, serializer()::decode, this::replace, serializer()::encode);
        executor.register(RaftConsistentMapOperations.REPLACE_VALUE, serializer()::decode, this::replaceValue, serializer()::encode);
        executor.register(RaftConsistentMapOperations.REPLACE_VERSION, serializer()::decode, this::replaceVersion, serializer()::encode);
        executor.register(RaftConsistentMapOperations.CLEAR, (Commit<Void> c) -> clear(), serializer()::encode);
        executor.register(RaftConsistentMapOperations.BEGIN, serializer()::decode, this::begin, serializer()::encode);
        executor.register(RaftConsistentMapOperations.PREPARE, serializer()::decode, this::prepare, serializer()::encode);
        executor.register(RaftConsistentMapOperations.PREPARE_AND_COMMIT, serializer()::decode, this::prepareAndCommit, serializer()::encode);
        executor.register(RaftConsistentMapOperations.COMMIT, serializer()::decode, this::commit, serializer()::encode);
        executor.register(RaftConsistentMapOperations.ROLLBACK, serializer()::decode, this::rollback, serializer()::encode);
    }

    /**
     * Handles a size commit.
     * @return number of entries in map
     */
    protected int size() {
        return (int) entries().values().stream()
            .filter(value -> value.type() != RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE)
            .count();
    }

    /**
     * Handles an is empty commit.
     * @return {@code true} if map is empty
     */
    protected boolean isEmpty() {
        return entries().values().stream()
            .noneMatch(value -> value.type() != RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE);
    }

    /**
     * Handles a keySet commit.
     * @return set of keys in map
     */
    protected Set<String> keySet() {
        return entries().entrySet().stream()
            .filter(entry -> entry.getValue().type() != RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    /**
     * Handles a values commit.
     * @return collection of values in map
     */
    protected Collection<Versioned<byte[]>> values() {
        return entries().entrySet().stream()
            .filter(entry -> entry.getValue().type() != RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE)
            .map(entry -> toVersioned(entry.getValue()))
            .collect(Collectors.toList());
    }

    /**
     * Handles a entry set commit.
     * @return set of map entries
     */
    protected Set<Map.Entry<String, Versioned<byte[]>>> entrySet() {
        return entries().entrySet().stream()
            .filter(entry -> entry.getValue().type() != RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE)
            .map(e -> Maps.immutableEntry(e.getKey(), toVersioned(e.getValue())))
            .collect(Collectors.toSet());
    }

    /**
     * Utility for turning a {@code MapEntryValue} to {@code Versioned}.
     * @param value map entry value
     * @return versioned instance
     */
    protected static Versioned<byte[]> toVersioned(RaftConsistentMapService.MapEntryValue value) {
        return value != null && value.type() != RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE
            ? new Versioned<>(value.value(), value.version()) : null;
    }

    /**
     * Handles a clear commit.
     * @return clear result
     */
    protected MapEntryUpdateResult.Status clear() {
        Iterator<Map.Entry<String, RaftConsistentMapService.MapEntryValue>> iterator = entries().entrySet().iterator();
        Map<String, RaftConsistentMapService.MapEntryValue> entriesToAdd = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<String, RaftConsistentMapService.MapEntryValue> entry = iterator.next();
            String key = entry.getKey();
            RaftConsistentMapService.MapEntryValue value = entry.getValue();
            if (!valueIsNull(value)) {
                Versioned<byte[]> removedValue = new Versioned<>(value.value(), value.version());
                publish(new MapEvent<>(MapEvent.Type.REMOVE, "", key, null, removedValue));
                if (activeTransactions.isEmpty()) {
                    iterator.remove();
                } else {
                    entriesToAdd.put(key, new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE, value.version, null));
                }
            }
        }
        entries().putAll(entriesToAdd);
        return MapEntryUpdateResult.Status.OK;
    }

    /**
     * Returns a boolean indicating whether the given MapEntryValue is null or a tombstone.
     * @param value the value to check
     * @return indicates whether the given value is null or is a tombstone
     */
    protected static boolean valueIsNull(RaftConsistentMapService.MapEntryValue value) {
        return value == null || value.type() == RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE;
    }

    /**
     * Publishes an event to listeners.
     * @param event event to publish
     */
    private void publish(MapEvent<String, byte[]> event) {
        publish(Lists.newArrayList(event));
    }

    /**
     * Publishes events to listeners.
     * @param events list of map event to publish
     */
    private void publish(List<MapEvent<String, byte[]>> events) {
        listeners.values().forEach(session -> {
            session.publish(RaftConsistentMapEvents.CHANGE, serializer()::encode, events);
        });
    }

    /**
     * Handles a listen commit.
     * @param session listen session
     */
    protected void listen(RaftSession session) {
        listeners.put(session.sessionId().id(), session);
    }

    /**
     * Handles an unlisten commit.
     * @param session unlisten session
     */
    protected void unlisten(RaftSession session) {
        listeners.remove(session.sessionId().id());
    }

    @Override
    public void onExpire(RaftSession session) {
        closeListener(session.sessionId().id());
    }

    @Override
    public void onClose(RaftSession session) {
        closeListener(session.sessionId().id());
    }

    private void closeListener(Long sessionId) {
        listeners.remove(sessionId);
    }

    /**
     * Handles a contains key commit.
     * @param commit containsKey commit
     * @return {@code true} if map contains key
     */
    protected boolean containsKey(Commit<? extends RaftConsistentMapOperations.ContainsKey> commit) {
        RaftConsistentMapService.MapEntryValue value = entries().get(commit.value().key());
        return value != null && value.type() != RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE;
    }

    /**
     * Handles a contains value commit.
     * @param commit containsValue commit
     * @return {@code true} if map contains value
     */
    protected boolean containsValue(Commit<? extends RaftConsistentMapOperations.ContainsValue> commit) {
        return entries().values().stream()
            .filter(value -> value.type() != RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE)
            .anyMatch(value -> Arrays.equals(value.value, commit.value().value()));
    }

    /**
     * Handles a get commit.
     * @param commit get commit
     * @return value mapped to key
     */
    protected Versioned<byte[]> get(Commit<? extends RaftConsistentMapOperations.Get> commit) {
        return toVersioned(entries().get(commit.value().key()));
    }

    /**
     * Handles a get all present commit.
     * @param commit get all present commit
     * @return keys present in map
     */
    protected Map<String, Versioned<byte[]>> getAllPresent(Commit<? extends RaftConsistentMapOperations.GetAllPresent> commit) {
        return entries().entrySet().stream()
            .filter(entry -> entry.getValue().type() != RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE
                && commit.value().keys().contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, o -> toVersioned(o.getValue())));
    }

    /**
     * Handles a get or default commit.
     * @param commit get or default commit
     * @return value mapped to key
     */
    protected Versioned<byte[]> getOrDefault(Commit<? extends RaftConsistentMapOperations.GetOrDefault> commit) {
        RaftConsistentMapService.MapEntryValue value = entries().get(commit.value().key());
        if (value == null) {
            return new Versioned<>(commit.value().defaultValue(), 0);
        } else if (value.type() == RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE) {
            return new Versioned<>(commit.value().defaultValue(), value.version);
        } else {
            return new Versioned<>(value.value(), value.version);
        }
    }

    /**
     * Handles a put commit.
     * @param commit put commit
     * @return map entry update result
     */
    protected MapEntryUpdateResult<String, byte[]> put(Commit<? extends RaftConsistentMapOperations.Put> commit) {
        String key = commit.value().key();
        RaftConsistentMapService.MapEntryValue oldValue = entries().get(key);
        RaftConsistentMapService.MapEntryValue newValue = new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.VALUE, commit.index(), commit.value().value());

        // If the value is null or a tombstone, this is an insert.
        // Otherwise, only update the value if it has changed to reduce the number of events.
        if (valueIsNull(oldValue)) {
            // If the key has been locked by a transaction, return a WRITE_LOCK error.
            if (preparedKeys.contains(key)) {
                return new MapEntryUpdateResult<>(
                    MapEntryUpdateResult.Status.WRITE_LOCK,
                    commit.index(),
                    key,
                    toVersioned(oldValue));
            }
            entries().put(commit.value().key(),
                new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.VALUE, newValue.version(), newValue.value()));
            Versioned<byte[]> result = toVersioned(oldValue);
            publish(new MapEvent<>(MapEvent.Type.INSERT, "", key, toVersioned(newValue), result));
            return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, commit.index(), key, result);
        } else if (!valuesEqual(oldValue, newValue)) {
            // If the key has been locked by a transaction, return a WRITE_LOCK error.
            if (preparedKeys.contains(key)) {
                return new MapEntryUpdateResult<>(
                    MapEntryUpdateResult.Status.WRITE_LOCK,
                    commit.index(),
                    key,
                    toVersioned(oldValue));
            }
            entries().put(commit.value().key(),
                new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.VALUE, newValue.version(), newValue.value()));
            Versioned<byte[]> result = toVersioned(oldValue);
            publish(new MapEvent<>(MapEvent.Type.UPDATE, "", key, toVersioned(newValue), result));
            return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, commit.index(), key, result);
        }
        // If the value hasn't changed, return a NOOP result.
        return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.NOOP, commit.index(), key, toVersioned(oldValue));
    }

    /**
     * Returns a boolean indicating whether the given MapEntryValues are equal.
     * @param oldValue the first value to compare
     * @param newValue the second value to compare
     * @return indicates whether the two values are equal
     */
    protected static boolean valuesEqual(RaftConsistentMapService.MapEntryValue oldValue, RaftConsistentMapService.MapEntryValue newValue) {
        return (oldValue == null && newValue == null)
            || (oldValue != null && newValue != null && valuesEqual(oldValue.value(), newValue.value()));
    }

    /**
     * Returns a boolean indicating whether the given entry values are equal.
     * @param oldValue the first value to compare
     * @param newValue the second value to compare
     * @return indicates whether the two values are equal
     */
    protected static boolean valuesEqual(byte[] oldValue, byte[] newValue) {
        return (oldValue == null && newValue == null)
            || (oldValue != null && newValue != null && Arrays.equals(oldValue, newValue));
    }

    /**
     * Handles a putIfAbsent commit.
     * @param commit putIfAbsent commit
     * @return map entry update result
     */
    protected MapEntryUpdateResult<String, byte[]> putIfAbsent(Commit<? extends RaftConsistentMapOperations.Put> commit) {
        String key = commit.value().key();
        RaftConsistentMapService.MapEntryValue oldValue = entries().get(key);

        // If the value is null, this is an INSERT.
        if (valueIsNull(oldValue)) {
            // If the key has been locked by a transaction, return a WRITE_LOCK error.
            if (preparedKeys.contains(key)) {
                return new MapEntryUpdateResult<>(
                    MapEntryUpdateResult.Status.WRITE_LOCK,
                    commit.index(),
                    key,
                    toVersioned(oldValue));
            }
            RaftConsistentMapService.MapEntryValue newValue = new RaftConsistentMapService.MapEntryValue(
                RaftConsistentMapService.MapEntryValue.Type.VALUE,
                commit.index(),
                commit.value().value());
            entries().put(commit.value().key(), newValue);
            Versioned<byte[]> result = toVersioned(newValue);
            publish(new MapEvent<>(MapEvent.Type.INSERT, "", key, result, null));
            return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, commit.index(), key, null);
        }
        return new MapEntryUpdateResult<>(
            MapEntryUpdateResult.Status.PRECONDITION_FAILED,
            commit.index(),
            key,
            toVersioned(oldValue));
    }

    /**
     * Handles a putAndGet commit.
     * @param commit putAndGet commit
     * @return map entry update result
     */
    protected MapEntryUpdateResult<String, byte[]> putAndGet(Commit<? extends RaftConsistentMapOperations.Put> commit) {
        String key = commit.value().key();
        RaftConsistentMapService.MapEntryValue oldValue = entries().get(key);
        RaftConsistentMapService.MapEntryValue newValue = new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.VALUE, commit.index(), commit.value().value());

        // If the value is null or a tombstone, this is an insert.
        // Otherwise, only update the value if it has changed to reduce the number of events.
        if (valueIsNull(oldValue)) {
            // If the key has been locked by a transaction, return a WRITE_LOCK error.
            if (preparedKeys.contains(key)) {
                return new MapEntryUpdateResult<>(
                    MapEntryUpdateResult.Status.WRITE_LOCK,
                    commit.index(),
                    key,
                    toVersioned(oldValue));
            }
            entries().put(commit.value().key(), newValue);
            Versioned<byte[]> result = toVersioned(newValue);
            publish(new MapEvent<>(MapEvent.Type.INSERT, "", key, result, null));
            return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, commit.index(), key, result);
        } else if (!valuesEqual(oldValue, newValue)) {
            // If the key has been locked by a transaction, return a WRITE_LOCK error.
            if (preparedKeys.contains(key)) {
                return new MapEntryUpdateResult<>(
                    MapEntryUpdateResult.Status.WRITE_LOCK,
                    commit.index(),
                    key,
                    toVersioned(oldValue));
            }
            entries().put(commit.value().key(), newValue);
            Versioned<byte[]> result = toVersioned(newValue);
            publish(new MapEvent<>(MapEvent.Type.UPDATE, "", key, result, toVersioned(oldValue)));
            return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, commit.index(), key, result);
        }
        return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.NOOP, commit.index(), key, toVersioned(oldValue));
    }

    /**
     * Handles a remove commit.
     * @param commit remove commit
     * @return map entry update result
     */
    protected MapEntryUpdateResult<String, byte[]> remove(Commit<? extends RaftConsistentMapOperations.Remove> commit) {
        return removeIf(commit.index(), commit.value().key(), v -> true);
    }

    /**
     * Handles a remove commit.
     * @param index the commit index
     * @param key the key to remove
     * @param predicate predicate to determine whether to remove the entry
     * @return map entry update result
     */
    private MapEntryUpdateResult<String, byte[]> removeIf(long index, String key, Predicate<RaftConsistentMapService.MapEntryValue> predicate) {
        RaftConsistentMapService.MapEntryValue value = entries().get(key);

        // If the value does not exist or doesn't match the predicate, return a PRECONDITION_FAILED error.
        if (valueIsNull(value) || !predicate.test(value)) {
            return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.PRECONDITION_FAILED, index, key, null);
        }

        // If the key has been locked by a transaction, return a WRITE_LOCK error.
        if (preparedKeys.contains(key)) {
            return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.WRITE_LOCK, index, key, null);
        }

        // If no transactions are active, remove the key. Otherwise, replace it with a tombstone.
        if (activeTransactions.isEmpty()) {
            entries().remove(key);
        } else {
            entries().put(key, new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE, index, null));
        }

        Versioned<byte[]> result = toVersioned(value);
        publish(new MapEvent<>(MapEvent.Type.REMOVE, "", key, null, result));
        return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, index, key, result);
    }

    /**
     * Handles a removeValue commit.
     * @param commit removeValue commit
     * @return map entry update result
     */
    protected MapEntryUpdateResult<String, byte[]> removeValue(Commit<? extends RaftConsistentMapOperations.RemoveValue> commit) {
        return removeIf(commit.index(), commit.value().key(), v ->
            valuesEqual(v, new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.VALUE, commit.index(), commit.value().value())));
    }

    /**
     * Handles a removeVersion commit.
     * @param commit removeVersion commit
     * @return map entry update result
     */
    protected MapEntryUpdateResult<String, byte[]> removeVersion(Commit<? extends RaftConsistentMapOperations.RemoveVersion> commit) {
        return removeIf(commit.index(), commit.value().key(), v -> v.version() == commit.value().version());
    }

    /**
     * Handles a replace commit.
     * @param commit replace commit
     * @return map entry update result
     */
    protected MapEntryUpdateResult<String, byte[]> replace(Commit<? extends RaftConsistentMapOperations.Replace> commit) {
        RaftConsistentMapService.MapEntryValue value = new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.VALUE, commit.index(), commit.value().value());
        return replaceIf(commit.index(), commit.value().key(), value, v -> true);
    }

    /**
     * Handles a replace commit.
     * @param index the commit index
     * @param key the key to replace
     * @param newValue the value with which to replace the key
     * @param predicate a predicate to determine whether to replace the key
     * @return map entry update result
     */
    private MapEntryUpdateResult<String, byte[]> replaceIf(
        long index, String key, RaftConsistentMapService.MapEntryValue newValue, Predicate<RaftConsistentMapService.MapEntryValue> predicate) {
        RaftConsistentMapService.MapEntryValue oldValue = entries().get(key);

        // If the key is not set or the current value doesn't match the predicate, return a PRECONDITION_FAILED error.
        if (valueIsNull(oldValue) || !predicate.test(oldValue)) {
            return new MapEntryUpdateResult<>(
                MapEntryUpdateResult.Status.PRECONDITION_FAILED,
                index,
                key,
                toVersioned(oldValue));
        }

        // If the key has been locked by a transaction, return a WRITE_LOCK error.
        if (preparedKeys.contains(key)) {
            return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.WRITE_LOCK, index, key, null);
        }

        entries().put(key, newValue);
        Versioned<byte[]> result = toVersioned(oldValue);
        publish(new MapEvent<>(MapEvent.Type.UPDATE, "", key, toVersioned(newValue), result));
        return new MapEntryUpdateResult<>(MapEntryUpdateResult.Status.OK, index, key, result);
    }

    /**
     * Handles a replaceValue commit.
     * @param commit replaceValue commit
     * @return map entry update result
     */
    protected MapEntryUpdateResult<String, byte[]> replaceValue(Commit<? extends RaftConsistentMapOperations.ReplaceValue> commit) {
        RaftConsistentMapService.MapEntryValue value = new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.VALUE, commit.index(), commit.value().newValue());
        return replaceIf(commit.index(), commit.value().key(), value,
            v -> valuesEqual(v.value(), commit.value().oldValue()));
    }

    /**
     * Handles a replaceVersion commit.
     * @param commit replaceVersion commit
     * @return map entry update result
     */
    protected MapEntryUpdateResult<String, byte[]> replaceVersion(Commit<? extends RaftConsistentMapOperations.ReplaceVersion> commit) {
        RaftConsistentMapService.MapEntryValue value = new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.VALUE, commit.index(), commit.value().newValue());
        return replaceIf(commit.index(), commit.value().key(), value,
            v -> v.version() == commit.value().oldVersion());
    }

    /**
     * Handles a begin commit.
     * @param commit transaction begin commit
     * @return transaction state version
     */
    protected long begin(Commit<? extends RaftConsistentMapOperations.TransactionBegin> commit) {
        long version = commit.index();
        activeTransactions.put(commit.value().transactionId(), new RaftConsistentMapService.TransactionScope(version));
        return version;
    }

    /**
     * Handles an prepare and commit commit.
     * @param commit transaction prepare and commit commit
     * @return prepare result
     */
    protected PrepareResult prepareAndCommit(Commit<? extends RaftConsistentMapOperations.TransactionPrepareAndCommit> commit) {
        TransactionId transactionId = commit.value().transactionLog().transactionId();
        PrepareResult prepareResult = prepare(commit);
        RaftConsistentMapService.TransactionScope transactionScope = activeTransactions.remove(transactionId);
        if (prepareResult == PrepareResult.OK) {
            this.currentVersion = commit.index();
            transactionScope = transactionScope.prepared(commit);
            commitTransaction(transactionScope);
        }
        discardTombstones();
        return prepareResult;
    }

    /**
     * Handles an prepare commit.
     * @param commit transaction prepare commit
     * @return prepare result
     */
    protected PrepareResult prepare(Commit<? extends RaftConsistentMapOperations.TransactionPrepare> commit) {
        try {
            TransactionLog<MapUpdate<String, byte[]>> transactionLog = commit.value().transactionLog();

            // Iterate through records in the transaction log and perform isolation checks.
            for (MapUpdate<String, byte[]> record : transactionLog.records()) {
                String key = record.key();

                // If the record is a VERSION_MATCH then check that the record's version matches the current
                // version of the state machine.
                if (record.type() == MapUpdate.Type.VERSION_MATCH && key == null) {
                    if (record.version() > currentVersion) {
                        return PrepareResult.OPTIMISTIC_LOCK_FAILURE;
                    } else {
                        continue;
                    }
                }

                // If the prepared keys already contains the key contained within the record, that indicates a
                // conflict with a concurrent transaction.
                if (preparedKeys.contains(key)) {
                    return PrepareResult.CONCURRENT_TRANSACTION;
                }

                // Read the existing value from the map.
                RaftConsistentMapService.MapEntryValue existingValue = entries().get(key);

                // Note: if the existing value is null, that means the key has not changed during the transaction,
                // otherwise a tombstone would have been retained.
                if (existingValue == null) {
                    // If the value is null, ensure the version is equal to the transaction version.
                    if (record.version() != transactionLog.version()) {
                        return PrepareResult.OPTIMISTIC_LOCK_FAILURE;
                    }
                } else {
                    // If the value is non-null, compare the current version with the record version.
                    if (existingValue.version() > record.version()) {
                        return PrepareResult.OPTIMISTIC_LOCK_FAILURE;
                    }
                }
            }

            // No violations detected. Mark modified keys locked for transactions.
            transactionLog.records().forEach(record -> {
                if (record.type() != MapUpdate.Type.VERSION_MATCH) {
                    preparedKeys.add(record.key());
                }
            });

            // Update the transaction scope. If the transaction scope is not set on this node, that indicates the
            // coordinator is communicating with another node. Transactions assume that the client is communicating
            // with a single leader in order to limit the overhead of retaining tombstones.
            RaftConsistentMapService.TransactionScope transactionScope = activeTransactions.get(transactionLog.transactionId());
            if (transactionScope == null) {
                activeTransactions.put(
                    transactionLog.transactionId(),
                    new RaftConsistentMapService.TransactionScope(transactionLog.version(), commit.value().transactionLog()));
                return PrepareResult.PARTIAL_FAILURE;
            } else {
                activeTransactions.put(
                    transactionLog.transactionId(),
                    transactionScope.prepared(commit));
                return PrepareResult.OK;
            }
        } catch (Exception e) {
            logger().warn("Failure applying {}", commit, e);
            throw Throwables.propagate(e);
        }
    }

    /**
     * Applies committed operations to the state machine.
     */
    private CommitResult commitTransaction(RaftConsistentMapService.TransactionScope transactionScope) {
        TransactionLog<MapUpdate<String, byte[]>> transactionLog = transactionScope.transactionLog();
        boolean retainTombstones = !activeTransactions.isEmpty();

        List<MapEvent<String, byte[]>> eventsToPublish = Lists.newArrayList();
        for (MapUpdate<String, byte[]> record : transactionLog.records()) {
            if (record.type() == MapUpdate.Type.VERSION_MATCH) {
                continue;
            }

            String key = record.key();
            Preconditions.checkState(preparedKeys.remove(key), "key is not prepared");

            if (record.type() == MapUpdate.Type.LOCK) {
                continue;
            }

            RaftConsistentMapService.MapEntryValue previousValue = entries().remove(key);
            RaftConsistentMapService.MapEntryValue newValue = null;

            // If the record is not a delete, create a transactional commit.
            if (record.type() != MapUpdate.Type.REMOVE_IF_VERSION_MATCH) {
                newValue = new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.VALUE, currentVersion, record.value());
            } else if (retainTombstones) {
                // For deletes, if tombstones need to be retained then create and store a tombstone commit.
                newValue = new RaftConsistentMapService.MapEntryValue(RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE, currentVersion, null);
            }

            MapEvent<String, byte[]> event;
            if (newValue != null) {
                entries().put(key, newValue);
                if (!valueIsNull(newValue)) {
                    if (!valueIsNull(previousValue)) {
                        event = new MapEvent<>(
                            MapEvent.Type.UPDATE,
                            "",
                            key,
                            toVersioned(newValue),
                            toVersioned(previousValue));
                    } else {
                        event = new MapEvent<>(
                            MapEvent.Type.INSERT,
                            "",
                            key,
                            toVersioned(newValue),
                            null);
                    }
                } else {
                    event = new MapEvent<>(
                        MapEvent.Type.REMOVE,
                        "",
                        key,
                        null,
                        toVersioned(previousValue));
                }
            } else {
                event = new MapEvent<>(
                    MapEvent.Type.REMOVE,
                    "",
                    key,
                    null,
                    toVersioned(previousValue));
            }
            eventsToPublish.add(event);
        }
        publish(eventsToPublish);
        return CommitResult.OK;
    }

    /**
     * Discards tombstones no longer needed by active transactions.
     */
    private void discardTombstones() {
        if (activeTransactions.isEmpty()) {
            Iterator<Map.Entry<String, RaftConsistentMapService.MapEntryValue>> iterator = entries().entrySet().iterator();
            while (iterator.hasNext()) {
                RaftConsistentMapService.MapEntryValue value = iterator.next().getValue();
                if (value.type() == RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE) {
                    iterator.remove();
                }
            }
        } else {
            long lowWaterMark = activeTransactions.values().stream()
                .mapToLong(RaftConsistentMapService.TransactionScope::version)
                .min().getAsLong();
            Iterator<Map.Entry<String, RaftConsistentMapService.MapEntryValue>> iterator = entries().entrySet().iterator();
            while (iterator.hasNext()) {
                RaftConsistentMapService.MapEntryValue value = iterator.next().getValue();
                if (value.type() == RaftConsistentMapService.MapEntryValue.Type.TOMBSTONE && value.version < lowWaterMark) {
                    iterator.remove();
                }
            }
        }
    }

    /**
     * Handles an commit commit (ha!).
     * @param commit transaction commit commit
     * @return commit result
     */
    protected CommitResult commit(Commit<? extends RaftConsistentMapOperations.TransactionCommit> commit) {
        TransactionId transactionId = commit.value().transactionId();
        RaftConsistentMapService.TransactionScope transactionScope = activeTransactions.remove(transactionId);
        if (transactionScope == null) {
            return CommitResult.UNKNOWN_TRANSACTION_ID;
        }

        try {
            this.currentVersion = commit.index();
            return commitTransaction(transactionScope);
        } catch (Exception e) {
            logger().warn("Failure applying {}", commit, e);
            throw Throwables.propagate(e);
        } finally {
            discardTombstones();
        }
    }

    /**
     * Handles an rollback commit (ha!).
     * @param commit transaction rollback commit
     * @return rollback result
     */
    protected RollbackResult rollback(Commit<? extends RaftConsistentMapOperations.TransactionRollback> commit) {
        TransactionId transactionId = commit.value().transactionId();
        RaftConsistentMapService.TransactionScope transactionScope = activeTransactions.remove(transactionId);
        if (transactionScope == null) {
            return RollbackResult.UNKNOWN_TRANSACTION_ID;
        } else if (!transactionScope.isPrepared()) {
            discardTombstones();
            return RollbackResult.OK;
        } else {
            try {
                transactionScope.transactionLog().records()
                    .forEach(record -> {
                        if (record.type() != MapUpdate.Type.VERSION_MATCH) {
                            preparedKeys.remove(record.key());
                        }
                    });
                return RollbackResult.OK;
            } finally {
                discardTombstones();
            }
        }

    }

    /**
     * Interface implemented by map values.
     */
    protected static class MapEntryValue {
        protected final RaftConsistentMapService.MapEntryValue.Type type;
        protected final long version;
        protected final byte[] value;

        MapEntryValue(RaftConsistentMapService.MapEntryValue.Type type, long version, byte[] value) {
            this.type = type;
            this.version = version;
            this.value = value;
        }

        /**
         * Returns the value type.
         * @return the value type
         */
        RaftConsistentMapService.MapEntryValue.Type type() {
            return type;
        }

        /**
         * Returns the version of the value.
         * @return version
         */
        long version() {
            return version;
        }

        /**
         * Returns the raw {@code byte[]}.
         * @return raw value
         */
        byte[] value() {
            return value;
        }

        /**
         * Value type.
         */
        enum Type {
            VALUE,
            TOMBSTONE,
        }
    }

    /**
     * Map transaction scope.
     */
    protected static final class TransactionScope {
        private final long version;
        private final TransactionLog<MapUpdate<String, byte[]>> transactionLog;

        private TransactionScope(long version) {
            this(version, null);
        }

        private TransactionScope(long version, TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
            this.version = version;
            this.transactionLog = transactionLog;
        }

        /**
         * Returns the transaction version.
         * @return the transaction version
         */
        long version() {
            return version;
        }

        /**
         * Returns the transaction commit log.
         * @return the transaction commit log
         */
        TransactionLog<MapUpdate<String, byte[]>> transactionLog() {
            Preconditions.checkState(isPrepared());
            return transactionLog;
        }

        /**
         * Returns whether this is a prepared transaction scope.
         * @return whether this is a prepared transaction scope
         */
        boolean isPrepared() {
            return transactionLog != null;
        }

        /**
         * Returns a new transaction scope with a prepare commit.
         * @param commit the prepare commit
         * @return new transaction scope updated with the prepare commit
         */
        RaftConsistentMapService.TransactionScope prepared(Commit<? extends RaftConsistentMapOperations.TransactionPrepare> commit) {
            return new RaftConsistentMapService.TransactionScope(version, commit.value().transactionLog());
        }
    }
}
