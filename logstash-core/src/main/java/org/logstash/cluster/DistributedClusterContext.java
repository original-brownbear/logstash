package org.logstash.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

final class DistributedClusterContext implements Closeable {

    private final transient UUID nodeId = UUID.randomUUID();

    private final BlockStore blockStore;

    private final String clusterName;

    private final DistributedMap nodes;

    DistributedClusterContext(final String clusterName, final BlockStore blockStore) {
        this.clusterName = clusterName;
        this.blockStore = blockStore;
        this.nodes = new DistributedMap(this, "clusterNodes");
    }

    String getClusterName() {
        return clusterName;
    }

    BlockStore getBlockStore() {
        return blockStore;
    }

    Iterable<SlaveNode> nodes() {
        final Collection<SlaveNode> result = new ArrayList<>();
        final Iterator<String> addresses = nodes.keyIterator();
        while (addresses.hasNext()) {
            final String[] parts = nodes.get(addresses.next()).split(":");
            result.add(new SlaveNode(new InetSocketAddress(parts[0], Integer.parseInt(parts[1]))));
        }
        return result;
    }

    void join(final InetSocketAddress address) {
        nodes.put(nodeId.toString(), address.getHostString().replaceFirst("/", "") + ':' + address.getPort());
    }

    @Override
    public void close() throws IOException {
        this.blockStore.close();
    }
}
