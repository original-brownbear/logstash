package org.logstash.cluster;

class TaskId {

    public final String clusterName;

    public final String pipeline;

    public final String identifier;

    TaskId(final String clusterName, final String pipeline, final String identifier) {
        this.clusterName = clusterName;
        this.pipeline = pipeline;
        this.identifier = identifier;
    }
}
