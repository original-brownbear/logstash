package org.logstash.cluster;

import java.io.DataInput;
import java.nio.file.Path;

final class LocalClusterLogState implements ClusterLogState {

    private final Path path;

    public static ClusterLogState fromDir(final Path path) {
        return new LocalClusterLogState(path);
    }

    private LocalClusterLogState(final Path path) {
        this.path = path;
    }

    @Override
    public long appliedIndex() {
        return 0L;
    }

    @Override
    public long commitIndex() {
        return 0L;
    }

    @Override
    public DataInput get(final byte[] key) {
        return null;
    }

    @Override
    public void put(final byte[] key, final DataInput value) {

    }

    @Override
    public void delete(final byte[] key) {

    }

    @Override
    public Iterable<byte[]> select(final byte[] key) {
        return null;
    }

    @Override
    public void apply(final LogEntry entry) {

    }
}
