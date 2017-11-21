package org.logstash.cluster;

public class DistributedSet {

    private final BlockStore blockStore;

    private final String name;

    DistributedSet(final BlockStore blockStore, final String name) {
        this.blockStore = blockStore;
        this.name = name;
    }

    public void put(final String key, final String value) {
        throw new UnsupportedOperationException();
    }

    public String get(final String key) {
        throw new UnsupportedOperationException();
    }

    public void delete() {
        throw new UnsupportedOperationException();
    }
}
