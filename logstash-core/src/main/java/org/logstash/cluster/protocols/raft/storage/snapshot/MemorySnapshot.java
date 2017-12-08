package org.logstash.cluster.protocols.raft.storage.snapshot;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.logstash.cluster.storage.StorageLevel;
import org.logstash.cluster.storage.buffer.HeapBuffer;

/**
 * In-memory snapshot backed by a {@link HeapBuffer}.
 */
final class MemorySnapshot extends Snapshot {
    private final String name;
    private final HeapBuffer buffer;
    private final SnapshotDescriptor descriptor;
    private final SnapshotStore store;

    MemorySnapshot(String name, HeapBuffer buffer, SnapshotDescriptor descriptor, SnapshotStore store) {
        super(descriptor, store);
        buffer.mark();
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
        this.buffer = Preconditions.checkNotNull(buffer, "buffer cannot be null");
        this.buffer.position(SnapshotDescriptor.BYTES).mark();
        this.descriptor = Preconditions.checkNotNull(descriptor, "descriptor cannot be null");
        this.store = Preconditions.checkNotNull(store, "store cannot be null");
    }

    @Override
    public String serviceName() {
        return name;
    }

    @Override
    public SnapshotWriter openWriter() {
        checkWriter();
        return new SnapshotWriter(buffer.reset().slice(), this);
    }

    @Override
    protected void closeWriter(SnapshotWriter writer) {
        buffer.skip(writer.buffer.position()).mark();
        super.closeWriter(writer);
    }

    @Override
    public synchronized SnapshotReader openReader() {
        return openReader(new SnapshotReader(buffer.reset().slice(), this), descriptor);
    }

    @Override
    public Snapshot complete() {
        buffer.flip().skip(SnapshotDescriptor.BYTES).mark();
        descriptor.lock();
        return super.complete();
    }

    @Override
    public Snapshot persist() {
        if (store.storage.storageLevel() != StorageLevel.MEMORY) {
            try (Snapshot newSnapshot = store.newSnapshot(serviceId(), name, index(), timestamp())) {
                try (SnapshotWriter newSnapshotWriter = newSnapshot.openWriter()) {
                    buffer.flip().skip(SnapshotDescriptor.BYTES);
                    newSnapshotWriter.write(buffer.array(), buffer.position(), buffer.remaining());
                }
                return newSnapshot;
            }
        }
        return this;
    }

    @Override
    public boolean isPersisted() {
        return store.storage.storageLevel() == StorageLevel.MEMORY;
    }

    @Override
    public void close() {
        buffer.close();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("index", index())
            .toString();
    }

}
