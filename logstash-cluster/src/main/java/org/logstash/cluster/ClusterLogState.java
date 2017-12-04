package org.logstash.cluster;

import java.io.DataInput;

interface ClusterLogState {

    long appliedIndex();

    long commitIndex();

    DataInput get(byte[] key);

    void put(byte[] key, DataInput value);

    void delete(byte[] key);

    Iterable<byte[]> select(byte[] key);

    void apply(LogEntry entry);
}
