package org.logstash.cluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.regex.Pattern;

public final class DistributedMap {

    private static final Pattern SUFFIX_PATTERN = Pattern.compile("/\\.entry$/");
    private final DistributedClusterContext context;

    private final String name;

    DistributedMap(final DistributedClusterContext context, final String name) {
        this.context = context;
        this.name = String.format("set%s", name);
    }

    public void put(final String key, final String value) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        buffer.put(value.getBytes(StandardCharsets.UTF_8)).flip();
        try {
            context.getBlockStore().store(entryBlockId(key), buffer);
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public String get(final String key) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        try {
            context.getBlockStore().load(entryBlockId(key), buffer);
            final byte[] data = new byte[buffer.flip().remaining()];
            buffer.get(data);
            return new String(data, StandardCharsets.UTF_8);
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public Iterator<String> keyIterator() {
        final Iterator<BlockId> blocks;
        try {
            blocks = context.getBlockStore().group(entryBlockId(""));
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return blocks.hasNext();
            }

            @Override
            public String next() {
                return SUFFIX_PATTERN.matcher(blocks.next().identifier).replaceAll("");
            }
        };
    }

    public void delete(final String key) {
        try {
            context.getBlockStore().delete(entryBlockId(key));
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private BlockId entryBlockId(final String key) {
        return new BlockId(context.getClusterName(), name, String.format(key, ".entry"));
    }
}
