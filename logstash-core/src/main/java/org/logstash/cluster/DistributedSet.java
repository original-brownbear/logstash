package org.logstash.cluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public final class DistributedSet {

    private final DistributedClusterContext context;

    private final String name;

    DistributedSet(final DistributedClusterContext context, final String name) {
        this.context = context;
        this.name = String.format("set%s", name);
    }

    public void put(final String key, final String value) {

    }

    public String get(final String key) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        try {
            if (context.getBlockStore().load(blockId(key), buffer) == 0L) {
                final byte[] data = new byte[buffer.flip().remaining()];
                buffer.get(data);
                return new String(data, StandardCharsets.UTF_8);
            }
            throw new IllegalStateException("Stored value too large");
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public Iterator<String> iterator() {
        return new DistributedSet.DistributedSetIterator();
    }

    public void delete(final String key) {
        try {
            context.getBlockStore().delete(blockId(key));
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private BlockId blockId(final String key) {
        return new BlockId(context.getClusterName(), name, String.format(key, ".entry"));
    }

    private final class DistributedSetIterator implements Iterator<String> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public String next() {
            return null;
        }
    }
}
