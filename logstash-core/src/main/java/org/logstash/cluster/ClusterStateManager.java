package org.logstash.cluster;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;

public final class ClusterStateManager {

    public static final InetAddress BIND_ADDRESSS = getBindAddress();

    public static final int BIND_PORT = getBindPort();

    private long commitIndex;

    private long appliedIndex;

    private final Path stateDir;

    public ClusterStateManager(final Path stateDir) {
        this.stateDir = stateDir;
        commitIndex = 0L;
        appliedIndex = 0L;
    }

    public void apply(final LogEntry entry) {

    }

    public int term() {
        return 0;
    }

    public String votedFor() {
        return "none";
    }

    private static InetAddress getBindAddress() {
        try {
            return InetAddress.getByName(
                System.getProperty(
                    "logstash.bind.address",
                    InetAddress.getLoopbackAddress().getHostAddress()
                )
            );
        } catch (final UnknownHostException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static int getBindPort() {
        return Integer.parseInt(System.getProperty("logstash.bind.port", "9700"));
    }
}
