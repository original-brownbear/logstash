package org.logstash.cluster;

import java.io.IOException;
import java.nio.ByteBuffer;

interface BlockStore {

    void store(BlockId key, ByteBuffer buffer) throws IOException;

    long load(BlockId key, ByteBuffer buffer) throws IOException;

    void delete(BlockId key) throws IOException;
}
