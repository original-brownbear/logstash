package org.logstash;

import java.io.IOException;
import java.net.ServerSocket;

public final class TestUtils {

    private TestUtils() {
        // Utility Class
    }

    public static int freePort() {
        try {
            try (ServerSocket socket = new ServerSocket(0)) {
                return socket.getLocalPort();
            }
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
