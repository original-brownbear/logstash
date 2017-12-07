/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package org.logstash.cluster.protocols.raft.storage.snapshot;

import com.google.common.base.Preconditions;
import java.nio.charset.Charset;
import java.util.function.Function;
import org.logstash.cluster.storage.buffer.Buffer;
import org.logstash.cluster.storage.buffer.BufferOutput;
import org.logstash.cluster.storage.buffer.Bytes;

/**
 * Writes bytes to a state machine {@link Snapshot}.
 * <p>
 * This class provides the primary interface for writing snapshot buffers to disk or memory.
 * Snapshot bytes are written to an underlying {@link Buffer} which is backed by either memory
 * or disk based on the configured {@link org.logstash.cluster.storage.StorageLevel}.
 * <p>
 * In addition to standard {@link BufferOutput} methods, snapshot readers support writing serializable objects
 * to the snapshot via the {@link #writeObject(Object, Function)} method. Serializable types must be registered on the
 * {@link org.logstash.cluster.protocols.raft.RaftServer} serializer to be supported in snapshots.
 */
public class SnapshotWriter implements BufferOutput<SnapshotWriter> {
    final Buffer buffer;
    private final Snapshot snapshot;

    SnapshotWriter(Buffer buffer, Snapshot snapshot) {
        this.buffer = Preconditions.checkNotNull(buffer, "buffer cannot be null");
        this.snapshot = Preconditions.checkNotNull(snapshot, "snapshot cannot be null");
    }

    /**
     * Returns the snapshot associated with the writer.
     * @return The snapshot associated with the writer
     */
    public Snapshot snapshot() {
        return snapshot;
    }

    /**
     * Writes an object to the snapshot.
     * @param object the object to write
     * @param encoder the object encoder
     * @return The snapshot writer.
     */
    public <T> SnapshotWriter writeObject(T object, Function<T, byte[]> encoder) {
        byte[] bytes = encoder.apply(object);
        buffer.writeInt(bytes.length).write(bytes);
        return this;
    }

    @Override
    public SnapshotWriter write(Bytes bytes) {
        buffer.write(bytes);
        return this;
    }

    @Override
    public SnapshotWriter write(Bytes bytes, int offset, int length) {
        buffer.write(bytes, offset, length);
        return this;
    }

    @Override
    public SnapshotWriter write(byte[] bytes, int offset, int length) {
        buffer.write(bytes, offset, length);
        return this;
    }

    @Override
    public SnapshotWriter write(Buffer buffer) {
        this.buffer.write(buffer);
        return this;
    }

    @Override
    public SnapshotWriter write(byte[] bytes) {
        buffer.write(bytes);
        return this;
    }

    @Override
    public SnapshotWriter writeByte(int b) {
        buffer.writeByte(b);
        return this;
    }

    @Override
    public SnapshotWriter writeUnsignedByte(int b) {
        buffer.writeUnsignedByte(b);
        return this;
    }

    @Override
    public SnapshotWriter writeChar(char c) {
        buffer.writeChar(c);
        return this;
    }

    @Override
    public SnapshotWriter writeShort(short s) {
        buffer.writeShort(s);
        return this;
    }

    @Override
    public SnapshotWriter writeUnsignedShort(int s) {
        buffer.writeUnsignedShort(s);
        return this;
    }

    @Override
    public SnapshotWriter writeMedium(int m) {
        buffer.writeMedium(m);
        return this;
    }

    @Override
    public SnapshotWriter writeUnsignedMedium(int m) {
        buffer.writeUnsignedMedium(m);
        return this;
    }

    @Override
    public SnapshotWriter writeInt(int i) {
        buffer.writeInt(i);
        return this;
    }

    @Override
    public SnapshotWriter writeUnsignedInt(long i) {
        buffer.writeUnsignedInt(i);
        return this;
    }

    @Override
    public SnapshotWriter writeLong(long l) {
        buffer.writeLong(l);
        return this;
    }

    @Override
    public SnapshotWriter writeFloat(float f) {
        buffer.writeFloat(f);
        return this;
    }

    @Override
    public SnapshotWriter writeDouble(double d) {
        buffer.writeDouble(d);
        return this;
    }

    @Override
    public SnapshotWriter writeBoolean(boolean b) {
        buffer.writeBoolean(b);
        return this;
    }

    @Override
    public SnapshotWriter writeString(String s) {
        buffer.writeString(s);
        return this;
    }

    @Override
    public SnapshotWriter writeString(String s, Charset charset) {
        buffer.writeString(s, charset);
        return this;
    }

    @Override
    public SnapshotWriter writeUTF8(String s) {
        buffer.writeUTF8(s);
        return this;
    }

    @Override
    public SnapshotWriter flush() {
        buffer.flush();
        return this;
    }

    @Override
    public void close() {
        snapshot.closeWriter(this);
        buffer.close();
    }

}