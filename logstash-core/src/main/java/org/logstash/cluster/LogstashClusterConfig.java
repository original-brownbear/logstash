package org.logstash.cluster;

import java.util.UUID;

public final class LogstashClusterConfig {

    private final String node;

    private final String esIndex;

    public LogstashClusterConfig(final String esIndex) {
        this(UUID.randomUUID().toString(), esIndex);
    }

    public LogstashClusterConfig(final String node, final String esIndex) {
        this.node = node;
        this.esIndex = esIndex;
    }

    public String esIndex() {
        return esIndex;
    }

    public String localNode() {
        return node;
    }
}
