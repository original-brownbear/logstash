package org.logstash.cluster.raft;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.net.InetSocketAddress;

public final class RaftRpcMessage implements Serializable {

    private final InetSocketAddress sender;

    private final long term;

    private final RaftRpcDirection direction;

    public RaftRpcMessage(final InetSocketAddress sender, final long term,
        final RaftRpcDirection direction) {
        this.sender = sender;
        this.term = term;
        this.direction = direction;
    }

    public InetSocketAddress getSender() {
        return sender;
    }

    public long getTerm() {
        return term;
    }

    public void writeTo(final ObjectOutput target) throws IOException {
        target.writeObject(this);
        target.flush();
    }
}
