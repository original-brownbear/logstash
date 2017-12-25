package org.logstash.cluster.protocols.raft.storage.snapshot;

import com.google.common.base.Preconditions;
import java.nio.charset.Charset;
import java.util.function.Function;
import org.logstash.cluster.storage.buffer.Buffer;
import org.logstash.cluster.storage.buffer.BufferInput;
import org.logstash.cluster.storage.buffer.Bytes;

/**
 * Reads bytes from a state machine {@link Snapshot}.
 * <p>
 * This class provides the primary interface for reading snapshot buffers from disk or memory.
 * Snapshot bytes are read from an underlying {@link Buffer} which is backed by either memory
 * or disk based on the configured {@link org.logstash.cluster.storage.StorageLevel}.
 * <p>
 * In addition to standard {@link BufferInput} methods, snapshot readers support reading serializable objects
 * from the snapshot via the {@link #readObject(Function)} method. Serializable types must be registered on the
 * {@link org.logstash.cluster.protocols.raft.RaftServer} serializer to be supported in snapshots.
 */
public class SnapshotReader implements BufferInput<SnapshotReader> {
    private final Buffer buffer;
    private final Snapshot snapshot;

    SnapshotReader(Buffer buffer, Snapshot snapshot) {
        this.buffer = Preconditions.checkNotNull(buffer, "buffer cannot be null");
        this.snapshot = Preconditions.checkNotNull(snapshot, "snapshot cannot be null");
    }

    /**
     * Returns the snapshot associated with the reader.
     * @return The snapshot associated with the reader
     */
    public Snapshot snapshot() {
        return snapshot;
    }

    @Override
    public int position() {
        return buffer.position();
    }

    @Override
    public int remaining() {
        return buffer.remaining();
    }

    @Override
    public boolean hasRemaining() {
        return buffer.hasRemaining();
    }

    @Override
    public SnapshotReader skip(int bytes) {
        buffer.skip(bytes);
        return this;
    }

    @Override
    public SnapshotReader read(Bytes bytes) {
        buffer.read(bytes);
        return this;
    }

    @Override
    public SnapshotReader read(Bytes bytes, int offset, int length) {
        buffer.read(bytes, offset, length);
        return this;
    }

    @Override
    public SnapshotReader read(byte[] bytes, int offset, int length) {
        buffer.read(bytes, offset, length);
        return this;
    }

    @Override
    public SnapshotReader read(Buffer buffer) {
        this.buffer.read(buffer);
        return this;
    }

    @Override
    public SnapshotReader read(byte[] bytes) {
        buffer.read(bytes);
        return this;
    }

    @Override
    public int readByte() {
        return buffer.readByte();
    }

    @Override
    public int readUnsignedByte() {
        return buffer.readUnsignedByte();
    }

    @Override
    public char readChar() {
        return buffer.readChar();
    }

    @Override
    public short readShort() {
        return buffer.readShort();
    }

    @Override
    public int readUnsignedShort() {
        return buffer.readUnsignedShort();
    }

    @Override
    public int readMedium() {
        return buffer.readMedium();
    }

    @Override
    public int readUnsignedMedium() {
        return buffer.readUnsignedMedium();
    }

    @Override
    public int readInt() {
        return buffer.readInt();
    }

    @Override
    public long readUnsignedInt() {
        return buffer.readUnsignedInt();
    }

    @Override
    public long readLong() {
        return buffer.readLong();
    }

    @Override
    public float readFloat() {
        return buffer.readFloat();
    }

    @Override
    public double readDouble() {
        return buffer.readDouble();
    }

    @Override
    public boolean readBoolean() {
        return buffer.readBoolean();
    }

    @Override
    public String readString() {
        return buffer.readString();
    }

    @Override
    public String readString(Charset charset) {
        return buffer.readString(charset);
    }

    @Override
    public String readUTF8() {
        return buffer.readUTF8();
    }

    @Override
    public void close() {
        buffer.close();
    }

    /**
     * Reads an object from the buffer.
     * @param decoder the object decoder
     * @param <T> the type of the object to read
     * @return the read object.
     */
    public <T> T readObject(Function<byte[], T> decoder) {
        byte[] bytes = buffer.readBytes(buffer.readInt());
        return decoder.apply(bytes);
    }

}
