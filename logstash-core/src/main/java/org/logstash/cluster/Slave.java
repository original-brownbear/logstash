package org.logstash.cluster;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.http.HttpHost;

public final class Slave {

    public static void main(final String... args) throws IOException, ClassNotFoundException {
        final DistributedClusterContext clusterContext = new DistributedClusterContext(
            "test", new EsBlockstore(HttpHost.create("http://127.0.0.1:9200"))
        );
        final ExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            clusterContext.join(new InetSocketAddress("127.0.0.1", serverSocket.getLocalPort()));
            while (true) {
                try (Socket master = serverSocket.accept()) {
                    final ObjectInputStream inputStream =
                        new ObjectInputStream(master.getInputStream());
                    final Iterable<LsClusterTask> tasks =
                        (Iterable<LsClusterTask>) inputStream.readObject();
                    tasks.forEach(exec::submit);
                }
            }
        }
    }
}
