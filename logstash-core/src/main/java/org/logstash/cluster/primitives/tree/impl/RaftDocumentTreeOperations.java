package org.logstash.cluster.primitives.tree.impl;

import com.google.common.base.MoreObjects;
import java.util.LinkedHashMap;
import java.util.Optional;
import org.logstash.cluster.primitives.TransactionId;
import org.logstash.cluster.primitives.TransactionLog;
import org.logstash.cluster.primitives.map.impl.CommitResult;
import org.logstash.cluster.primitives.map.impl.PrepareResult;
import org.logstash.cluster.primitives.map.impl.RollbackResult;
import org.logstash.cluster.primitives.tree.DocumentPath;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.time.Versioned;
import org.logstash.cluster.utils.Match;

/**
 * {@link RaftDocumentTree} resource state machine operations.
 */
public enum RaftDocumentTreeOperations implements OperationId {
    ADD_LISTENER("set", OperationType.COMMAND),
    REMOVE_LISTENER("compareAndSet", OperationType.COMMAND),
    GET("incrementAndGet", OperationType.QUERY),
    GET_CHILDREN("getAndIncrement", OperationType.QUERY),
    UPDATE("addAndGet", OperationType.COMMAND),
    CLEAR("getAndAdd", OperationType.COMMAND),
    BEGIN("begin", OperationType.COMMAND),
    PREPARE("prepare", OperationType.COMMAND),
    PREPARE_AND_COMMIT("prepareAndCommit", OperationType.COMMAND),
    COMMIT("commit", OperationType.COMMAND),
    ROLLBACK("rollback", OperationType.COMMAND);

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
        .register(LinkedHashMap.class)
        .register(RaftDocumentTreeOperations.Listen.class)
        .register(RaftDocumentTreeOperations.Unlisten.class)
        .register(RaftDocumentTreeOperations.Get.class)
        .register(RaftDocumentTreeOperations.GetChildren.class)
        .register(RaftDocumentTreeOperations.Update.class)
        .register(TransactionId.class)
        .register(TransactionLog.class)
        .register(PrepareResult.class)
        .register(CommitResult.class)
        .register(RollbackResult.class)
        .register(NodeUpdate.class)
        .register(NodeUpdate.Type.class)
        .register(DocumentPath.class)
        .register(Match.class)
        .register(Versioned.class)
        .register(DocumentTreeResult.class)
        .register(DocumentTreeResult.Status.class)
        .build(RaftDocumentTreeOperations.class.getSimpleName());
    private final String id;
    private final OperationType type;

    RaftDocumentTreeOperations(String id, OperationType type) {
        this.id = id;
        this.type = type;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public OperationType type() {
        return type;
    }

    /**
     * Base class for document tree operations.
     */
    public abstract static class DocumentTreeOperation {
    }

    /**
     * Base class for document tree operations that serialize a {@link DocumentPath}.
     */
    @SuppressWarnings("serial")
    public abstract static class PathOperation extends RaftDocumentTreeOperations.DocumentTreeOperation {
        private final DocumentPath path;

        PathOperation(DocumentPath path) {
            this.path = path;
        }

        public DocumentPath path() {
            return path;
        }
    }

    /**
     * DocumentTree#get query.
     */
    @SuppressWarnings("serial")
    public static class Get extends RaftDocumentTreeOperations.PathOperation {
        public Get() {
            super(null);
        }

        public Get(DocumentPath path) {
            super(path);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("path", path())
                .toString();
        }
    }

    /**
     * DocumentTree#getChildren query.
     */
    @SuppressWarnings("serial")
    public static class GetChildren extends RaftDocumentTreeOperations.PathOperation {
        public GetChildren() {
            super(null);
        }

        public GetChildren(DocumentPath path) {
            super(path);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("path", path())
                .toString();
        }
    }

    /**
     * DocumentTree update command.
     */
    @SuppressWarnings("serial")
    public static class Update extends RaftDocumentTreeOperations.PathOperation {
        private final Optional<byte[]> value;
        private final Match<byte[]> valueMatch;
        private final Match<Long> versionMatch;

        public Update() {
            super(null);
            this.value = null;
            this.valueMatch = null;
            this.versionMatch = null;
        }

        public Update(DocumentPath path, Optional<byte[]> value, Match<byte[]> valueMatch, Match<Long> versionMatch) {
            super(path);
            this.value = value;
            this.valueMatch = valueMatch;
            this.versionMatch = versionMatch;
        }

        public Optional<byte[]> value() {
            return value;
        }

        public Match<byte[]> valueMatch() {
            return valueMatch;
        }

        public Match<Long> versionMatch() {
            return versionMatch;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("path", path())
                .add("value", value)
                .add("valueMatch", valueMatch)
                .add("versionMatch", versionMatch)
                .toString();
        }
    }

    /**
     * Change listen.
     */
    @SuppressWarnings("serial")
    public static class Listen extends RaftDocumentTreeOperations.PathOperation {
        public Listen() {
            this(DocumentPath.from("root"));
        }

        public Listen(DocumentPath path) {
            super(path);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("path", path())
                .toString();
        }
    }

    /**
     * Change unlisten.
     */
    @SuppressWarnings("serial")
    public static class Unlisten extends RaftDocumentTreeOperations.PathOperation {
        public Unlisten() {
            this(DocumentPath.from("root"));
        }

        public Unlisten(DocumentPath path) {
            super(path);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("path", path())
                .toString();
        }
    }
}
