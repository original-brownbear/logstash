package org.logstash.cluster;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public final class PortUtil {

    public static InetSocketAddress randomLoopbackAddress() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), reserve());
    }

    public static int reserve() {
        synchronized (PortUtil.class) {
            try (final ServerSocket socket = new ServerSocket(0)) {
                return socket.getLocalPort();
            } catch (final IOException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }
}
