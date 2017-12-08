package org.logstash.cluster.storage.buffer;

import java.nio.ByteOrder;
import org.logstash.cluster.utils.concurrent.ReferenceManager;

/**
 * Byte order swapped buffer.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SwappedBuffer extends AbstractBuffer {
    private final Buffer root;

    public SwappedBuffer(Buffer buffer, int offset, int initialCapacity, int maxCapacity, ReferenceManager<Buffer> referenceManager) {
        super(buffer.bytes().order(buffer.order() == ByteOrder.BIG_ENDIAN ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN), offset, initialCapacity, maxCapacity, referenceManager);
        this.root = buffer instanceof SwappedBuffer ? ((SwappedBuffer) buffer).root : buffer;
        root.acquire();
    }

    /**
     * Returns the root buffer.
     * @return The root buffer.
     */
    public Buffer root() {
        return root;
    }

    @Override
    public Buffer duplicate() {
        return new SwappedBuffer(root, offset(), capacity(), maxCapacity(), referenceManager);
    }

    @Override
    public Buffer acquire() {
        root.acquire();
        return this;
    }

    @Override
    public boolean release() {
        return root.release();
    }

    @Override
    public boolean isDirect() {
        return root.isDirect();
    }

    @Override
    public boolean isFile() {
        return root.isFile();
    }

    @Override
    public boolean isReadOnly() {
        return root.isReadOnly();
    }

    @Override
    protected void compact(int from, int to, int length) {
        if (root instanceof AbstractBuffer) {
            ((AbstractBuffer) root).compact(from, to, length);
        }
    }

    @Override
    public void close() {
        root.release();
    }

}
