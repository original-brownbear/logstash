package org.logstash.cluster;

class BlockId {

    public final String clusterName;

    public final String pipeline;

    public final String identifier;

    BlockId(final String clusterName, final String pipeline, final String identifier) {
        this.clusterName = clusterName;
        this.pipeline = pipeline;
        this.identifier = identifier;
    }
}
