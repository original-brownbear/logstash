package org.logstash.cluster;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.http.HttpHost;

public final class LogstashClusterConfig {

    private final String node;

    private final String esIndex;

    private final Collection<HttpHost> esHosts;

    public LogstashClusterConfig(final String esIndex, final Collection<HttpHost> esHosts) {
        this(UUID.randomUUID().toString(), esIndex, esHosts);
    }

    public LogstashClusterConfig(final String node, final String esIndex,
        final Collection<HttpHost> esHosts) {
        this.node = node;
        this.esIndex = esIndex;
        this.esHosts = esHosts;
    }

    public String esIndex() {
        return esIndex;
    }

    public Collection<HttpHost> esHosts() {
        return Collections.unmodifiableCollection(esHosts);
    }

    public String localNode() {
        return node;
    }

}
