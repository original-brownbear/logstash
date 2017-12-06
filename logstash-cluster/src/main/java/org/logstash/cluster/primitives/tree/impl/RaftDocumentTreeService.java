/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.logstash.cluster.primitives.tree.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.logstash.cluster.primitives.Ordering;
import org.logstash.cluster.primitives.tree.DocumentPath;
import org.logstash.cluster.primitives.tree.DocumentTree;
import org.logstash.cluster.primitives.tree.DocumentTreeEvent;
import org.logstash.cluster.primitives.tree.IllegalDocumentModificationException;
import org.logstash.cluster.primitives.tree.NoSuchDocumentPathException;
import org.logstash.cluster.protocols.raft.event.EventType;
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

import static org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeEvents.CHANGE;
import static org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeOperations.ADD_LISTENER;
import static org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeOperations.CLEAR;
import static org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeOperations.GET;
import static org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeOperations.GET_CHILDREN;
import static org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeOperations.REMOVE_LISTENER;
import static org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeOperations.UPDATE;

/**
 * State Machine for {@link RaftDocumentTree} resource.
 */
public class RaftDocumentTreeService extends AbstractRaftService {
    private Map<Long, SessionListenCommits> listeners = new HashMap<>();
    private AtomicLong versionCounter = new AtomicLong(0);
    private final Serializer serializer = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftDocumentTreeOperations.NAMESPACE)
        .register(RaftDocumentTreeEvents.NAMESPACE)
        .register(new com.esotericsoftware.kryo.Serializer<Listener>() {
            @Override
            public void write(Kryo kryo, Output output, Listener listener) {
                output.writeLong(listener.session.sessionId().id());
                kryo.writeObject(output, listener.path);
            }

            @Override
            public Listener read(Kryo kryo, Input input, Class<Listener> type) {
                return new Listener(sessions().getSession(input.readLong()),
                    kryo.readObjectOrNull(input, DocumentPath.class));
            }
        }, Listener.class)
        .register(Versioned.class)
        .register(DocumentPath.class)
        .register(new LinkedHashMap().keySet().getClass())
        .register(TreeMap.class)
        .register(Ordering.class)
        .register(SessionListenCommits.class)
        .register(new com.esotericsoftware.kryo.Serializer<DefaultDocumentTree>() {
            @Override
            public void write(Kryo kryo, Output output, DefaultDocumentTree object) {
                kryo.writeObject(output, object.root);
            }

            @Override
            @SuppressWarnings("unchecked")
            public DefaultDocumentTree read(Kryo kryo, Input input, Class<DefaultDocumentTree> type) {
                return new DefaultDocumentTree(versionCounter::incrementAndGet,
                    kryo.readObject(input, DefaultDocumentTreeNode.class));
            }
        }, DefaultDocumentTree.class)
        .register(DefaultDocumentTreeNode.class)
        .build());
    private DocumentTree<byte[]> docTree;
    private Set<DocumentPath> preparedKeys = Sets.newHashSet();

    public RaftDocumentTreeService(Ordering ordering) {
        this.docTree = new DefaultDocumentTree<>(versionCounter::incrementAndGet, ordering);
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeLong(versionCounter.get());
        writer.writeObject(listeners, serializer::encode);
        writer.writeObject(docTree, serializer::encode);
        writer.writeObject(preparedKeys, serializer::encode);
    }

    @Override
    public void install(SnapshotReader reader) {
        versionCounter = new AtomicLong(reader.readLong());
        listeners = reader.readObject(serializer::decode);
        docTree = reader.readObject(serializer::decode);
        preparedKeys = reader.readObject(serializer::decode);
    }

    @Override
    protected void configure(RaftServiceExecutor executor) {
        // Listeners
        executor.register(ADD_LISTENER, serializer::decode, this::listen);
        executor.register(REMOVE_LISTENER, serializer::decode, this::unlisten);
        // queries
        executor.register(GET, serializer::decode, this::get, serializer::encode);
        executor.register(GET_CHILDREN, serializer::decode, this::getChildren, serializer::encode);
        // commands
        executor.register(UPDATE, serializer::decode, this::update, serializer::encode);
        executor.register(CLEAR, this::clear);
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

    protected void listen(Commit<? extends RaftDocumentTreeOperations.Listen> commit) {
        Long sessionId = commit.session().sessionId().id();
        listeners.computeIfAbsent(sessionId, k -> new SessionListenCommits())
            .add(new Listener(commit.session(), commit.value().path()));
    }

    protected void unlisten(Commit<? extends RaftDocumentTreeOperations.Unlisten> commit) {
        Long sessionId = commit.session().sessionId().id();
        SessionListenCommits listenCommits = listeners.get(sessionId);
        if (listenCommits != null) {
            listenCommits.remove(commit);
        }
    }

    protected Versioned<byte[]> get(Commit<? extends RaftDocumentTreeOperations.Get> commit) {
        try {
            Versioned<byte[]> value = docTree.get(commit.value().path());
            return value == null ? null : value.map(node -> node);
        } catch (IllegalStateException e) {
            return null;
        }
    }

    protected DocumentTreeResult<Map<String, Versioned<byte[]>>> getChildren(Commit<? extends RaftDocumentTreeOperations.GetChildren> commit) {
        try {
            return DocumentTreeResult.ok(docTree.getChildren(commit.value().path()));
        } catch (NoSuchDocumentPathException e) {
            return DocumentTreeResult.invalidPath();
        }
    }

    protected DocumentTreeResult<Versioned<byte[]>> update(Commit<? extends RaftDocumentTreeOperations.Update> commit) {
        DocumentTreeResult<Versioned<byte[]>> result = null;
        DocumentPath path = commit.value().path();

        // If the path is locked by a transaction, return a WRITE_LOCK error.
        if (isLocked(path)) {
            return DocumentTreeResult.writeLock();
        }

        Versioned<byte[]> currentValue = docTree.get(path);
        try {
            Match<Long> versionMatch = commit.value().versionMatch();
            Match<byte[]> valueMatch = commit.value().valueMatch();

            if (versionMatch.matches(currentValue == null ? null : currentValue.version())
                && valueMatch.matches(currentValue == null ? null : currentValue.value())) {
                if (commit.value().value() == null) {
                    Versioned<byte[]> oldValue = docTree.removeNode(path);
                    result = new DocumentTreeResult<>(DocumentTreeResult.Status.OK, oldValue);
                    if (oldValue != null) {
                        notifyListeners(new DocumentTreeEvent<>(
                            path,
                            DocumentTreeEvent.Type.DELETED,
                            Optional.empty(),
                            Optional.of(oldValue)));
                    }
                } else {
                    Versioned<byte[]> oldValue = docTree.set(path, commit.value().value().orElse(null));
                    Versioned<byte[]> newValue = docTree.get(path);
                    result = new DocumentTreeResult<>(DocumentTreeResult.Status.OK, newValue);
                    if (oldValue == null) {
                        notifyListeners(new DocumentTreeEvent<>(
                            path,
                            DocumentTreeEvent.Type.CREATED,
                            Optional.of(newValue),
                            Optional.empty()));
                    } else {
                        notifyListeners(new DocumentTreeEvent<>(
                            path,
                            DocumentTreeEvent.Type.UPDATED,
                            Optional.of(newValue),
                            Optional.of(oldValue)));
                    }
                }
            } else {
                result = new DocumentTreeResult<>(
                    commit.value().value() == null ? DocumentTreeResult.Status.INVALID_PATH : DocumentTreeResult.Status.NOOP,
                    currentValue);
            }
        } catch (IllegalDocumentModificationException e) {
            result = DocumentTreeResult.illegalModification();
        } catch (NoSuchDocumentPathException e) {
            result = DocumentTreeResult.invalidPath();
        } catch (Exception e) {
            logger().error("Failed to apply {} to state machine", commit.value(), e);
            throw Throwables.propagate(e);
        }
        return result;
    }

    /**
     * Returns a boolean indicating whether the given path is currently locked by a transaction.
     * @param path the path to check
     * @return whether the given path is locked by a running transaction
     */
    private boolean isLocked(DocumentPath path) {
        return preparedKeys.contains(path);
    }

    private void notifyListeners(DocumentTreeEvent<byte[]> event) {
        listeners.values()
            .stream()
            .filter(l -> event.path().isDescendentOf(l.leastCommonAncestorPath()))
            .forEach(listener -> listener.publish(CHANGE, Arrays.asList(event)));
    }

    protected void clear(Commit<Void> commit) {
        Queue<DocumentPath> toClearQueue = Queues.newArrayDeque();
        Map<String, Versioned<byte[]>> topLevelChildren = docTree.getChildren(DocumentPath.from("root"));
        toClearQueue.addAll(topLevelChildren.keySet()
            .stream()
            .map(name -> new DocumentPath(name, DocumentPath.from("root")))
            .collect(Collectors.toList()));
        while (!toClearQueue.isEmpty()) {
            DocumentPath path = toClearQueue.remove();
            Map<String, Versioned<byte[]>> children = docTree.getChildren(path);
            if (children.size() == 0) {
                docTree.removeNode(path);
            } else {
                children.keySet().forEach(name -> toClearQueue.add(new DocumentPath(name, path)));
                toClearQueue.add(path);
            }
        }
    }

    private void publish(List<DocumentTreeEvent<byte[]>> events) {
        listeners.values().forEach(session -> {
            session.publish(CHANGE, events);
        });
    }

    private static class Listener {
        private final RaftSession session;
        private final DocumentPath path;

        public Listener(RaftSession session, DocumentPath path) {
            this.session = session;
            this.path = path;
        }

        public DocumentPath path() {
            return path;
        }

        public RaftSession session() {
            return session;
        }
    }

    private class SessionListenCommits {
        private final List<Listener> listeners = Lists.newArrayList();
        private DocumentPath leastCommonAncestorPath;

        public void add(Listener listener) {
            listeners.add(listener);
            recomputeLeastCommonAncestor();
        }

        private void recomputeLeastCommonAncestor() {
            this.leastCommonAncestorPath = DocumentPath.leastCommonAncestor(listeners.stream()
                .map(Listener::path)
                .collect(Collectors.toList()));
        }

        public void remove(Commit<? extends RaftDocumentTreeOperations.Unlisten> commit) {
            // Remove the first listen commit with path matching path in unlisten commit
            Iterator<Listener> iterator = listeners.iterator();
            while (iterator.hasNext()) {
                Listener listener = iterator.next();
                if (listener.path().equals(commit.value().path())) {
                    iterator.remove();
                }
            }
            recomputeLeastCommonAncestor();
        }

        public DocumentPath leastCommonAncestorPath() {
            return leastCommonAncestorPath;
        }

        public <M> void publish(EventType topic, M message) {
            listeners.stream().findAny().ifPresent(listener ->
                listener.session().publish(topic, serializer::encode, message));
        }
    }
}
