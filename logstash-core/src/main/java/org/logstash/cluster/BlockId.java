package org.logstash.cluster;

class BlockId {

    public final String clusterName;

    public final String identifier;

    public final String group;

    BlockId(final String clusterName, final String group, final String identifier) {
        this.clusterName = clusterName;
        this.group = group;
        this.identifier = identifier;
    }
}
