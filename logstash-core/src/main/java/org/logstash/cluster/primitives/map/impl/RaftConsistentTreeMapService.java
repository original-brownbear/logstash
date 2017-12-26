package org.logstash.cluster.primitives.map.impl;

import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.logstash.cluster.primitives.TransactionId;
import org.logstash.cluster.primitives.TransactionLog;
import org.logstash.cluster.protocols.raft.service.Commit;
import org.logstash.cluster.protocols.raft.service.RaftServiceExecutor;
import org.logstash.cluster.protocols.raft.session.RaftSession;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.time.Versioned;

/**
 * State machine corresponding to {@link RaftConsistentTreeMap} backed by a
 * {@link TreeMap}.
 */
public class RaftConsistentTreeMapService extends RaftConsistentMapService {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftConsistentMapOperations.NAMESPACE)
        .register(RaftConsistentTreeMapOperations.NAMESPACE)
        .register(RaftConsistentMapEvents.NAMESPACE)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 150)
        .register(RaftConsistentMapService.TransactionScope.class)
        .register(TransactionLog.class)
        .register(TransactionId.class)
        .register(RaftConsistentMapService.MapEntryValue.class)
        .register(RaftConsistentMapService.MapEntryValue.Type.class)
        .register(new HashMap().keySet().getClass())
        .register(TreeMap.class)
        .build());

    @Override
    protected TreeMap<String, RaftConsistentMapService.MapEntryValue> createMap() {
        return Maps.newTreeMap();
    }

    @Override
    protected TreeMap<String, RaftConsistentMapService.MapEntryValue> entries() {
        return (TreeMap<String, RaftConsistentMapService.MapEntryValue>) super.entries();
    }

    @Override
    protected Serializer serializer() {
        return SERIALIZER;
    }

    @Override
    public void configure(RaftServiceExecutor executor) {
        super.configure(executor);
        executor.register(RaftConsistentTreeMapOperations.SUB_MAP, serializer()::decode, this::subMap, serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.FIRST_KEY, (Commit<Void> c) -> firstKey(), serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.LAST_KEY, (Commit<Void> c) -> lastKey(), serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.FIRST_ENTRY, (Commit<Void> c) -> firstEntry(), serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.LAST_ENTRY, (Commit<Void> c) -> lastEntry(), serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.POLL_FIRST_ENTRY, (Commit<Void> c) -> pollFirstEntry(), serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.POLL_LAST_ENTRY, (Commit<Void> c) -> pollLastEntry(), serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.LOWER_ENTRY, serializer()::decode, this::lowerEntry, serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.LOWER_KEY, serializer()::decode, this::lowerKey, serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.FLOOR_ENTRY, serializer()::decode, this::floorEntry, serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.FLOOR_KEY, serializer()::decode, this::floorKey, serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.CEILING_ENTRY, serializer()::decode, this::ceilingEntry, serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.CEILING_KEY, serializer()::decode, this::ceilingKey, serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.HIGHER_ENTRY, serializer()::decode, this::higherEntry, serializer()::encode);
        executor.register(RaftConsistentTreeMapOperations.HIGHER_KEY, serializer()::decode, this::higherKey, serializer()::encode);
    }

    protected String firstKey() {
        return isEmpty() ? null : entries().firstKey();
    }

    protected String lastKey() {
        return isEmpty() ? null : entries().lastKey();
    }

    protected Map.Entry<String, Versioned<byte[]>> firstEntry() {
        return isEmpty() ? null : toVersionedEntry(entries().firstEntry());
    }

    private static Map.Entry<String, Versioned<byte[]>> toVersionedEntry(
        Map.Entry<String, RaftConsistentMapService.MapEntryValue> entry) {
        return entry == null || valueIsNull(entry.getValue())
            ? null : Maps.immutableEntry(entry.getKey(), toVersioned(entry.getValue()));
    }

    protected Map.Entry<String, Versioned<byte[]>> lastEntry() {
        return isEmpty() ? null : toVersionedEntry(entries().lastEntry());
    }

    protected Map.Entry<String, Versioned<byte[]>> pollFirstEntry() {
        return toVersionedEntry(entries().pollFirstEntry());
    }

    protected Map.Entry<String, Versioned<byte[]>> pollLastEntry() {
        return toVersionedEntry(entries().pollLastEntry());
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

    protected NavigableMap<String, RaftConsistentMapService.MapEntryValue> subMap(
        Commit<? extends RaftConsistentTreeMapOperations.SubMap> commit) {
        // Do not support this until lazy communication is possible.  At present
        // it transmits up to the entire map.
        RaftConsistentTreeMapOperations.SubMap<String, RaftConsistentMapService.MapEntryValue> subMap = commit.value();
        return entries().subMap(subMap.fromKey(), subMap.isInclusiveFrom(),
            subMap.toKey(), subMap.isInclusiveTo());
    }

    protected Map.Entry<String, Versioned<byte[]>> higherEntry(Commit<? extends RaftConsistentTreeMapOperations.HigherEntry> commit) {
        return isEmpty() ? null : toVersionedEntry(entries().higherEntry(commit.value().key()));
    }

    protected Map.Entry<String, Versioned<byte[]>> lowerEntry(Commit<? extends RaftConsistentTreeMapOperations.LowerEntry> commit) {
        return toVersionedEntry(entries().lowerEntry(commit.value().key()));
    }

    protected String lowerKey(Commit<? extends RaftConsistentTreeMapOperations.LowerKey> commit) {
        return entries().lowerKey(commit.value().key());
    }

    protected Map.Entry<String, Versioned<byte[]>> floorEntry(Commit<? extends RaftConsistentTreeMapOperations.FloorEntry> commit) {
        return toVersionedEntry(entries().floorEntry(commit.value().key()));
    }

    protected String floorKey(Commit<? extends RaftConsistentTreeMapOperations.FloorKey> commit) {
        return entries().floorKey(commit.value().key());
    }

    protected Map.Entry<String, Versioned<byte[]>> ceilingEntry(Commit<RaftConsistentTreeMapOperations.CeilingEntry> commit) {
        return toVersionedEntry(entries().ceilingEntry(commit.value().key()));
    }

    protected String ceilingKey(Commit<RaftConsistentTreeMapOperations.CeilingKey> commit) {
        return entries().ceilingKey(commit.value().key());
    }

    protected String higherKey(Commit<RaftConsistentTreeMapOperations.HigherKey> commit) {
        return entries().higherKey(commit.value().key());
    }
}
