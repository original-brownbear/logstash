package org.logstash.cluster.primitives.multimap.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.logstash.cluster.primitives.multimap.MultimapEvent;
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
import org.logstash.cluster.utils.Match;

/**
 * State Machine for {@link RaftConsistentSetMultimap} resource.
 */
public class RaftConsistentSetMultimapService extends AbstractRaftService {

    private final Serializer serializer = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftConsistentSetMultimapOperations.NAMESPACE)
        .register(RaftConsistentSetMultimapEvents.NAMESPACE)
        .register(RaftConsistentSetMultimapService.ByteArrayComparator.class)
        .register(new HashMap().keySet().getClass())
        .register(TreeSet.class)
        .register(new com.esotericsoftware.kryo.Serializer<RaftConsistentSetMultimapService.NonTransactionalCommit>() {
            @Override
            public void write(Kryo kryo, Output output, RaftConsistentSetMultimapService.NonTransactionalCommit object) {
                kryo.writeClassAndObject(output, object.valueSet);
            }

            @Override
            @SuppressWarnings("unchecked")
            public RaftConsistentSetMultimapService.NonTransactionalCommit read(Kryo kryo, Input input, Class<RaftConsistentSetMultimapService.NonTransactionalCommit> type) {
                RaftConsistentSetMultimapService.NonTransactionalCommit commit = new RaftConsistentSetMultimapService.NonTransactionalCommit();
                commit.valueSet.addAll((Collection<byte[]>) kryo.readClassAndObject(input));
                return commit;
            }
        }, RaftConsistentSetMultimapService.NonTransactionalCommit.class)
        .build());

    private AtomicLong globalVersion = new AtomicLong(1);
    private Map<Long, RaftSession> listeners = new LinkedHashMap<>();
    private Map<String, RaftConsistentSetMultimapService.MapEntryValue> backingMap = Maps.newHashMap();

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeLong(globalVersion.get());
        writer.writeObject(Sets.newHashSet(listeners.keySet()), serializer::encode);
        writer.writeObject(backingMap, serializer::encode);
    }

    @Override
    public void install(SnapshotReader reader) {
        globalVersion = new AtomicLong(reader.readLong());

        listeners = new LinkedHashMap<>();
        for (Long sessionId : reader.<Set<Long>>readObject(serializer::decode)) {
            listeners.put(sessionId, sessions().getSession(sessionId));
        }

        backingMap = reader.readObject(serializer::decode);
    }

    @Override
    protected void configure(RaftServiceExecutor executor) {
        executor.register(RaftConsistentSetMultimapOperations.SIZE, this::size, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.IS_EMPTY, this::isEmpty, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.CONTAINS_KEY, serializer::decode, this::containsKey, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.CONTAINS_VALUE, serializer::decode, this::containsValue, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.CONTAINS_ENTRY, serializer::decode, this::containsEntry, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.CLEAR, this::clear);
        executor.register(RaftConsistentSetMultimapOperations.KEY_SET, this::keySet, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.KEYS, this::keys, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.VALUES, this::values, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.ENTRIES, this::entries, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.GET, serializer::decode, this::get, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.REMOVE_ALL, serializer::decode, this::removeAll, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.REMOVE, serializer::decode, this::multiRemove, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.PUT, serializer::decode, this::put, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.REPLACE, serializer::decode, this::replace, serializer::encode);
        executor.register(RaftConsistentSetMultimapOperations.ADD_LISTENER, this::listen);
        executor.register(RaftConsistentSetMultimapOperations.REMOVE_LISTENER, this::unlisten);
    }

    @Override
    public void onExpire(RaftSession session) {
        listeners.remove(session.sessionId().id());
    }

    @Override
    public void onClose(RaftSession session) {
        listeners.remove(session.sessionId().id());
    }

    /**
     * Handles a Size commit.
     * @param commit Size commit
     * @return number of unique key value pairs in the multimap
     */
    protected int size(Commit<Void> commit) {
        return backingMap.values()
            .stream()
            .map(valueCollection -> valueCollection.values().size()).mapToInt(size -> size).sum();
    }

    /**
     * Handles an IsEmpty commit.
     * @param commit IsEmpty commit
     * @return true if the multimap contains no key-value pairs, else false
     */
    protected boolean isEmpty(Commit<Void> commit) {
        return backingMap.isEmpty();
    }

    /**
     * Handles a contains key commit.
     * @param commit ContainsKey commit
     * @return returns true if the key is in the multimap, else false
     */
    protected boolean containsKey(Commit<? extends RaftConsistentSetMultimapOperations.ContainsKey> commit) {
        return backingMap.containsKey(commit.value().key());
    }

    /**
     * Handles a ContainsValue commit.
     * @param commit ContainsValue commit
     * @return true if the value is in the multimap, else false
     */
    protected boolean containsValue(Commit<? extends RaftConsistentSetMultimapOperations.ContainsValue> commit) {
        if (backingMap.values().isEmpty()) {
            return false;
        }
        Match<byte[]> match = Match.ifValue(commit.value().value());
        return backingMap
            .values()
            .stream()
            .anyMatch(valueList ->
                valueList
                    .values()
                    .stream()
                    .anyMatch(byteValue ->
                        match.matches(byteValue)));
    }

    /**
     * Handles a ContainsEntry commit.
     * @param commit ContainsEntry commit
     * @return true if the key-value pair exists, else false
     */
    protected boolean containsEntry(Commit<? extends RaftConsistentSetMultimapOperations.ContainsEntry> commit) {
        RaftConsistentSetMultimapService.MapEntryValue entryValue =
            backingMap.get(commit.value().key());
        if (entryValue == null) {
            return false;
        } else {
            Match valueMatch = Match.ifValue(commit.value().value());
            return entryValue
                .values()
                .stream()
                .anyMatch(byteValue -> valueMatch.matches(byteValue));
        }
    }

    /**
     * Handles a Clear commit.
     * @param commit Clear commit
     */
    protected void clear(Commit<Void> commit) {
        backingMap.clear();
    }

    /**
     * Handles a KeySet commit.
     * @param commit KeySet commit
     * @return a set of all keys in the multimap
     */
    protected Set<String> keySet(Commit<Void> commit) {
        return ImmutableSet.copyOf(backingMap.keySet());
    }

    /**
     * Handles a Keys commit.
     * @param commit Keys commit
     * @return a multiset of keys with each key included an equal number of
     * times to the total key-value pairs in which that key participates
     */
    protected Multiset<String> keys(Commit<Void> commit) {
        Multiset keys = HashMultiset.create();
        backingMap.forEach((key, mapEntryValue) -> {
            keys.add(key, mapEntryValue.values().size());
        });
        return keys;
    }

    /**
     * Handles a Values commit.
     * @param commit Values commit
     * @return the set of values in the multimap with duplicates included
     */
    protected Multiset<byte[]> values(Commit<Void> commit) {
        return backingMap
            .values()
            .stream()
            .collect(new RaftConsistentSetMultimapService.HashMultisetValueCollector());
    }

    /**
     * Handles an Entries commit.
     * @param commit Entries commit
     * @return a set of all key-value pairs in the multimap
     */
    protected Collection<Map.Entry<String, byte[]>> entries(Commit<Void> commit) {
        return backingMap
            .entrySet()
            .stream()
            .collect(new RaftConsistentSetMultimapService.EntrySetCollector());
    }

    /**
     * Handles a Get commit.
     * @param commit Get commit
     * @return the collection of values associated with the key or an empty
     * list if none exist
     */
    protected Versioned<Collection<? extends byte[]>> get(Commit<? extends RaftConsistentSetMultimapOperations.Get> commit) {
        return toVersioned(backingMap.get(commit.value().key()));
    }

    /**
     * Utility for turning a {@code MapEntryValue} to {@code Versioned}.
     * @param value map entry value
     * @return versioned instance or an empty list versioned -1 if argument is
     * null
     */
    private static Versioned<Collection<? extends byte[]>> toVersioned(
        RaftConsistentSetMultimapService.MapEntryValue value) {
        return value == null ? new Versioned<>(Lists.newArrayList(), -1) :
            new Versioned<>(value.values(),
                value.version());
    }

    /**
     * Handles a removeAll commit, and returns the previous mapping.
     * @param commit removeAll commit
     * @return collection of removed values
     */
    protected Versioned<Collection<? extends byte[]>> removeAll(Commit<? extends RaftConsistentSetMultimapOperations.RemoveAll> commit) {
        String key = commit.value().key();

        if (!backingMap.containsKey(key)) {
            return new Versioned<>(Sets.newHashSet(), -1);
        }

        Versioned<Collection<? extends byte[]>> removedValues =
            backingMap.get(key).addCommit(commit);
        publish(removedValues.value().stream()
            .map(value -> new MultimapEvent<String, byte[]>(
                "", key, null, value))
            .collect(Collectors.toList()));
        return removedValues;
    }

    /**
     * Publishes events to listeners.
     * @param events list of map event to publish
     */
    private void publish(List<MultimapEvent<String, byte[]>> events) {
        listeners.values().forEach(session -> session.publish(RaftConsistentSetMultimapEvents.CHANGE, serializer::encode, events));
    }

    /**
     * Handles a multiRemove commit, returns true if the remove results in any
     * change.
     * @param commit multiRemove commit
     * @return true if any change results, else false
     */
    protected boolean multiRemove(Commit<? extends RaftConsistentSetMultimapOperations.MultiRemove> commit) {
        String key = commit.value().key();

        if (!backingMap.containsKey(key)) {
            return false;
        }

        Versioned<Collection<? extends byte[]>> removedValues = backingMap
            .get(key)
            .addCommit(commit);

        if (removedValues != null) {
            if (removedValues.value().isEmpty()) {
                backingMap.remove(key);
            }

            publish(removedValues.value().stream()
                .map(value -> new MultimapEvent<String, byte[]>(
                    "", key, null, value))
                .collect(Collectors.toList()));
            return true;
        }

        return false;
    }

    /**
     * Handles a put commit, returns true if any change results from this
     * commit.
     * @param commit a put commit
     * @return true if this commit results in a change, else false
     */
    protected boolean put(Commit<? extends RaftConsistentSetMultimapOperations.Put> commit) {
        String key = commit.value().key();
        if (commit.value().values().isEmpty()) {
            return false;
        }
        if (!backingMap.containsKey(key)) {
            backingMap.put(key, new RaftConsistentSetMultimapService.NonTransactionalCommit());
        }

        Versioned<Collection<? extends byte[]>> addedValues = backingMap
            .get(key)
            .addCommit(commit);

        if (addedValues != null) {
            publish(addedValues.value().stream()
                .map(value -> new MultimapEvent<String, byte[]>(
                    "", key, value, null))
                .collect(Collectors.toList()));
            return true;
        }

        return false;
    }

    protected Versioned<Collection<? extends byte[]>> replace(
        Commit<? extends RaftConsistentSetMultimapOperations.Replace> commit) {
        if (!backingMap.containsKey(commit.value().key())) {
            backingMap.put(commit.value().key(),
                new RaftConsistentSetMultimapService.NonTransactionalCommit());
        }
        return backingMap.get(commit.value().key()).addCommit(commit);
    }

    /**
     * Handles a listen commit.
     * @param commit listen commit
     */
    protected void listen(Commit<Void> commit) {
        listeners.put(commit.session().sessionId().id(), commit.session());
    }

    /**
     * Handles an unlisten commit.
     * @param commit unlisten commit
     */
    protected void unlisten(Commit<Void> commit) {
        listeners.remove(commit.session().sessionId().id());
    }

    private interface MapEntryValue {

        /**
         * Returns the list of raw {@code byte[]'s}.
         * @return list of raw values
         */
        Collection<? extends byte[]> values();

        /**
         * Returns the version of the value.
         * @return version
         */
        long version();

        /**
         * Add a new commit and modifies the set of values accordingly.
         * In the case of a replace or removeAll it returns the set of removed
         * values. In the case of put or multiRemove it returns null for no
         * change and a set of the added or removed values respectively if a
         * change resulted.
         * @param commit the commit to be added
         */
        Versioned<Collection<? extends byte[]>> addCommit(
            Commit<? extends RaftConsistentSetMultimapOperations.MultimapOperation> commit);
    }

    private static class ByteArrayComparator implements Comparator<byte[]> {

        @Override
        public int compare(byte[] o1, byte[] o2) {
            if (Arrays.equals(o1, o2)) {
                return 0;
            } else {
                for (int i = 0; i < o1.length && i < o2.length; i++) {
                    if (o1[i] < o2[i]) {
                        return -1;
                    } else if (o1[i] > o2[i]) {
                        return 1;
                    }
                }
                return o1.length > o2.length ? 1 : -1;
            }
        }
    }

    private class NonTransactionalCommit implements RaftConsistentSetMultimapService.MapEntryValue {
        private final TreeSet<byte[]> valueSet = Sets.newTreeSet(new RaftConsistentSetMultimapService.ByteArrayComparator());
        private long version;

        public NonTransactionalCommit() {
            //Set the version to current it will only be updated once this is
            // populated
            this.version = globalVersion.get();
        }

        @Override
        public Collection<? extends byte[]> values() {
            return ImmutableSet.copyOf(valueSet);
        }

        @Override
        public long version() {
            return version;
        }

        @Override
        public Versioned<Collection<? extends byte[]>> addCommit(
            Commit<? extends RaftConsistentSetMultimapOperations.MultimapOperation> commit) {
            Preconditions.checkNotNull(commit);
            Preconditions.checkNotNull(commit.value());
            Versioned<Collection<? extends byte[]>> retVersion;

            if (commit.value() instanceof RaftConsistentSetMultimapOperations.Put) {
                //Using a treeset here sanitizes the input, removing duplicates
                Set<byte[]> valuesToAdd =
                    Sets.newTreeSet(new RaftConsistentSetMultimapService.ByteArrayComparator());
                ((RaftConsistentSetMultimapOperations.Put) commit.value()).values().forEach(value -> {
                    if (!valueSet.contains(value)) {
                        valuesToAdd.add(value);
                    }
                });
                if (valuesToAdd.isEmpty()) {
                    //Do not increment or add the commit if no change resulted
                    return null;
                }
                retVersion = new Versioned<>(valuesToAdd, version);
                valuesToAdd.forEach(value -> valueSet.add(value));
                version++;
                return retVersion;

            } else if (commit.value() instanceof RaftConsistentSetMultimapOperations.Replace) {
                //Will this work??  Need to check before check-in!
                Set<byte[]> removedValues = Sets.newHashSet();
                removedValues.addAll(valueSet);
                retVersion = new Versioned<>(removedValues, version);
                valueSet.clear();
                Set<byte[]> valuesToAdd =
                    Sets.newTreeSet(new RaftConsistentSetMultimapService.ByteArrayComparator());
                ((RaftConsistentSetMultimapOperations.Replace) commit.value()).values().forEach(value -> {
                    valuesToAdd.add(value);
                });
                if (valuesToAdd.isEmpty()) {
                    version = globalVersion.incrementAndGet();
                    backingMap.remove(((RaftConsistentSetMultimapOperations.Replace) commit.value()).key());
                    return retVersion;
                }
                valuesToAdd.forEach(value -> valueSet.add(value));
                version = globalVersion.incrementAndGet();
                return retVersion;

            } else if (commit.value() instanceof RaftConsistentSetMultimapOperations.RemoveAll) {
                Set<byte[]> removed = Sets.newHashSet();
                //We can assume here that values only appear once and so we
                //do not need to sanitize the return for duplicates.
                removed.addAll(valueSet);
                retVersion = new Versioned<>(removed, version);
                valueSet.clear();
                //In the case of a removeAll all commits will be removed and
                //unlike the multiRemove case we do not need to consider
                //dependencies among additive and removal commits.

                //Save the key for use after the commit is closed
                String key = ((RaftConsistentSetMultimapOperations.RemoveAll) commit.value()).key();
                version = globalVersion.incrementAndGet();
                backingMap.remove(key);
                return retVersion;

            } else if (commit.value() instanceof RaftConsistentSetMultimapOperations.MultiRemove) {
                //Must first calculate how many commits the removal depends on.
                //At this time we also sanitize the removal set by adding to a
                //set with proper handling of byte[] equality.
                Set<byte[]> removed = Sets.newHashSet();
                ((RaftConsistentSetMultimapOperations.MultiRemove) commit.value()).values().forEach(value -> {
                    if (valueSet.contains(value)) {
                        removed.add(value);
                    }
                });
                //If there is nothing to be removed no action should be taken.
                if (removed.isEmpty()) {
                    return null;
                }
                //Save key in case countdown results in closing the commit.
                String removedKey = ((RaftConsistentSetMultimapOperations.MultiRemove) commit.value()).key();
                removed.forEach(removedValue -> {
                    valueSet.remove(removedValue);
                });
                //The version is updated locally as well as globally even if
                //this object will be removed from the map in case any other
                //party still holds a reference to this object.
                retVersion = new Versioned<>(removed, version);
                version = globalVersion.incrementAndGet();
                if (valueSet.isEmpty()) {
                    backingMap.remove(removedKey);
                }
                return retVersion;

            } else {
                throw new IllegalArgumentException();
            }
        }
    }

    /**
     * A collector that creates MapEntryValues and creates a multiset of all
     * values in the map an equal number of times to the number of sets in
     * which they participate.
     */
    private static class HashMultisetValueCollector implements
        Collector<RaftConsistentSetMultimapService.MapEntryValue,
            HashMultiset<byte[]>,
            HashMultiset<byte[]>> {

        @Override
        public Supplier<HashMultiset<byte[]>> supplier() {
            return HashMultiset::create;
        }

        @Override
        public BiConsumer<HashMultiset<byte[]>, RaftConsistentSetMultimapService.MapEntryValue> accumulator() {
            return (multiset, mapEntryValue) ->
                multiset.addAll(mapEntryValue.values());
        }

        @Override
        public BinaryOperator<HashMultiset<byte[]>> combiner() {
            return (setOne, setTwo) -> {
                setOne.addAll(setTwo);
                return setOne;
            };
        }

        @Override
        public Function<HashMultiset<byte[]>,
            HashMultiset<byte[]>> finisher() {
            return Function.identity();
        }

        @Override
        public Set<Collector.Characteristics> characteristics() {
            return EnumSet.of(Collector.Characteristics.UNORDERED);
        }
    }

    /**
     * A collector that creates Entries of {@code <String, MapEntryValue>} and
     * creates a set of entries all key value pairs in the map.
     */
    private static class EntrySetCollector implements
        Collector<Map.Entry<String, RaftConsistentSetMultimapService.MapEntryValue>,
            Set<Map.Entry<String, byte[]>>,
            Set<Map.Entry<String, byte[]>>> {
        private Set<Map.Entry<String, byte[]>> set = null;

        @Override
        public Supplier<Set<Map.Entry<String, byte[]>>> supplier() {
            return () -> {
                if (set == null) {
                    set = Sets.newHashSet();
                }
                return set;
            };
        }

        @Override
        public BiConsumer<Set<Map.Entry<String, byte[]>>,
            Map.Entry<String, RaftConsistentSetMultimapService.MapEntryValue>> accumulator() {
            return (set, entry) -> {
                entry
                    .getValue()
                    .values()
                    .forEach(byteValue ->
                        set.add(Maps.immutableEntry(entry.getKey(),
                            byteValue)));
            };
        }

        @Override
        public BinaryOperator<Set<Map.Entry<String, byte[]>>> combiner() {
            return (setOne, setTwo) -> {
                setOne.addAll(setTwo);
                return setOne;
            };
        }

        @Override
        public Function<Set<Map.Entry<String, byte[]>>,
            Set<Map.Entry<String, byte[]>>> finisher() {
            return (unused) -> set;
        }

        @Override
        public Set<Collector.Characteristics> characteristics() {
            return EnumSet.of(Collector.Characteristics.UNORDERED);
        }
    }
}
