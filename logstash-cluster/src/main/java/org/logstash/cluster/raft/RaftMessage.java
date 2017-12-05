package org.logstash.cluster.raft;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.net.InetSocketAddress;

public final class RaftMessage implements Serializable {

    private final InetSocketAddress sender;

    public RaftMessage(final InetSocketAddress sender) {
        this.sender = sender;
    }

    public InetSocketAddress getSender() {
        return sender;
    }

    public void writeTo(final ObjectOutput target) throws IOException {
        target.writeObject(this);
        target.flush();
    }
}
