package org.logstash.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

interface BlockStore extends Closeable {

    void store(BlockId key, ByteBuffer buffer) throws IOException;

    Void load(BlockId key, ByteBuffer buffer) throws IOException;

    void delete(BlockId key) throws IOException;

    Iterator<BlockId> group(final BlockId key) throws IOException;

}
