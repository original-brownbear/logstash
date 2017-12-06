package org.logstash.cluster.messaging;

import com.google.common.base.Preconditions;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 * Representation of a TCP/UDP communication end point.
 */
public final class Endpoint {

    private final int port;
    private final InetAddress ip;
    public Endpoint(InetAddress host, int port) {
        this.ip = Preconditions.checkNotNull(host);
        this.port = port;
    }

    /**
     * Returns an endpoint for the given host/port.
     * @param host the host
     * @param port the port
     * @return a new endpoint
     */
    public static Endpoint from(String host, int port) {
        try {
            return new Endpoint(InetAddress.getByName(host), port);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Failed to locate host", e);
        }
    }

    public int port() {
        return port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Endpoint that = (Endpoint) obj;
        return this.port == that.port &&
            Objects.equals(this.ip, that.ip);
    }

    @Override
    public String toString() {
        return String.format("%s:%d", host().getHostAddress(), port);
    }

    public InetAddress host() {
        return ip;
    }
}
