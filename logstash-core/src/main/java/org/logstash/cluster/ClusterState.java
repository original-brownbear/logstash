package org.logstash.cluster;

import java.io.DataInput;

interface ClusterState {

    long appliedIndex();

    DataInput get(byte[] key);

    void put(byte[] key, DataInput value);

    void delete(byte[] key);

    Iterable<byte[]> select(byte[] key);
}
