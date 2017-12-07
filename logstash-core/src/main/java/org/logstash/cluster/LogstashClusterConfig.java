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

    public LogstashClusterConfig(final InetSocketAddress bind, final Collection<Node> bootstrap,
        final File dataDir) {
        this.node = new DefaultNode(
            NodeId.from(bind.toString()), new Endpoint(bind.getAddress(), bind.getPort())
        );
        this.bootstrap = bootstrap.isEmpty() ? Collections.singleton(node) : bootstrap;
        this.dataDir = dataDir;
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
