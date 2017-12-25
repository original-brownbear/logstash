package org.logstash.cluster.storage.buffer;

/**
 * Sliced buffer.
 * <p>
 * The sliced buffer provides a view of a subset of an underlying buffer. This buffer operates directly on the {@link Bytes}
 * underlying the child {@link Buffer} instance.
 */
public class SlicedBuffer extends AbstractBuffer {
    private final Buffer root;

    public SlicedBuffer(Buffer root, Bytes bytes, int offset, int initialCapacity, int maxCapacity) {
        super(bytes, offset, initialCapacity, maxCapacity, null);
        this.root = root;
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
        return new SlicedBuffer(root, bytes, offset(), capacity(), maxCapacity());
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
    public Buffer compact() {
        return null;
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
