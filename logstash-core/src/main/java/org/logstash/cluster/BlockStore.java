package org.logstash.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

interface BlockStore extends Closeable {

    void store(BlockId key, ByteBuffer buffer) throws IOException;

    long load(BlockId key, ByteBuffer buffer) throws IOException;

    void delete(BlockId key) throws IOException;
}
