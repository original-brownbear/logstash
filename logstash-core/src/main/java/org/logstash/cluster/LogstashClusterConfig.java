package org.logstash.cluster;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.cluster.impl.DefaultNode;
import org.logstash.cluster.messaging.Endpoint;

public final class LogstashClusterConfig {

    private final Node node;

    private final Collection<Node> bootstrap;

    private final File dataDir;

    private final String esIndex;

    public LogstashClusterConfig(final String nodeId, final InetSocketAddress bind,
        final Collection<Node> bootstrap, final File dataDir, final String esIndex) {
        this(new DefaultNode(
            NodeId.from(nodeId), new Endpoint(bind.getAddress(), bind.getPort())
        ), bootstrap, dataDir, esIndex);
    }

    public LogstashClusterConfig(final Node node, final Collection<Node> bootstrap,
        final File dataDir, final String esIndex) {
        this.node = node;
        this.bootstrap = bootstrap.isEmpty() ? Collections.singleton(node) : bootstrap;
        this.dataDir = dataDir;
        this.esIndex = esIndex;
    }

    public LogstashClusterConfig withBootstrap(final Collection<Node> bootstrap) {
        return new LogstashClusterConfig(node, bootstrap, dataDir, esIndex);
    }

    public String esIndex() {
        return esIndex;
    }

    public Collection<Node> getBootstrap() {
        return Collections.unmodifiableCollection(bootstrap);
    }

    public Node localNode() {
        return node;
    }

    public File getDataDir() {
        return dataDir;
    }
}
