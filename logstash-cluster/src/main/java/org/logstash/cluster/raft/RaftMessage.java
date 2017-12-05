package org.logstash.cluster.raft;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.net.InetSocketAddress;

public final class RaftMessage implements Serializable {

    private final InetSocketAddress sender;

    private final long term;

    public RaftMessage(final InetSocketAddress sender, final long term) {
        this.sender = sender;
        this.term = term;
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
