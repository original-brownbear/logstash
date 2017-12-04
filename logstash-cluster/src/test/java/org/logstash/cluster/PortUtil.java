package org.logstash.cluster;

import java.io.IOException;
import java.net.ServerSocket;

public final class PortUtil {
    /**
     * Reserve new port for each call.
     * @return Reserved port.
     */
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
