package org.logstash.cluster.storage.buffer;

import org.logstash.cluster.utils.concurrent.ReferenceManager;

/**
 * {@link java.nio.ByteBuffer} based buffer.
 */
public abstract class ByteBufferBuffer extends AbstractBuffer {
    protected final ByteBufferBytes bytes;

    public ByteBufferBuffer(ByteBufferBytes bytes, int offset, int initialCapacity, int maxCapacity, ReferenceManager<Buffer> referenceManager) {
        super(bytes, offset, initialCapacity, maxCapacity, referenceManager);
        this.bytes = bytes;
    }

    @Override
    public byte[] array() {
        return bytes.array();
    }

    @Override
    protected void compact(int from, int to, int length) {
        byte[] bytes = new byte[1024];
        int position = from;
        while (position < from + length) {
            int size = Math.min((from + length) - position, 1024);
            this.bytes.read(position, bytes, 0, size);
            this.bytes.write(0, bytes, 0, size);
            position += size;
        }
    }

}
