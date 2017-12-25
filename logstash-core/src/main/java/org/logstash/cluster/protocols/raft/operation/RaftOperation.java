package org.logstash.cluster.protocols.raft.operation;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * Base type for Raft state operations.
 */
public class RaftOperation {
    protected final OperationId id;
    protected final byte[] value;

    protected RaftOperation() {
        this.id = null;
        this.value = null;
    }

    public RaftOperation(OperationId id, byte[] value) {
        this.id = id;
        this.value = value;
    }

    /**
     * Returns the operation identifier.
     * @return the operation identifier
     */
    public OperationId id() {
        return id;
    }

    /**
     * Returns the operation value.
     * @return the operation value
     */
    public byte[] value() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), id, value);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof RaftOperation) {
            RaftOperation operation = (RaftOperation) object;
            return Objects.equals(operation.id, id) && Objects.equals(operation.value, value);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("value", ArraySizeHashPrinter.of(value))
            .toString();
    }
}
