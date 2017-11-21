package org.logstash.cluster;

import java.net.InetSocketAddress;

final class SlaveNode {

    private final InetSocketAddress address;

    SlaveNode(final InetSocketAddress address) {
        this.address = address;
    }

    public InetSocketAddress getAddress() {
        return address;
    }
}
