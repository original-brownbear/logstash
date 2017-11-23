package org.logstash.cluster;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.HttpHost;

public final class Master {

    public static void main(final String... args) throws IOException, InterruptedException {
        System.out.println("Master PID " + ManagementFactory.getRuntimeMXBean().getName());
        final ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        final int masterPort = 7723;
        try (
            BlockStore blockStore = new EsBlockstore(HttpHost.create("http://127.0.0.1:9200"));
            DistributedClusterContext clusterContext =
                new DistributedClusterContext("test", blockStore);
            Master.LeaderService coordinator = new Master.LeaderService(masterPort)
        ) {
            for (final SlaveNode slave : clusterContext.nodes()) {
                System.out.println(String.format("Found peer: %s", slave.getAddress().toString()));
            }
            executorService.submit(coordinator);
            TimeUnit.SECONDS.sleep(5L);
            final DistributedPipelineContext context =
                new DistributedPipelineContext(new InetSocketAddress(masterPort), clusterContext);
            final Collection<LsClusterTask> tasks = Arrays.asList(
                () -> {
                    System.out.println("Starting tasks");
                    try {
                        TimeUnit.SECONDS.sleep(5L);
                    } catch (final InterruptedException ex) {
                        throw new IllegalStateException(ex);
                    }
                },
                () -> System.out.println("Doing task on" + ManagementFactory.getRuntimeMXBean().getName()),
                () -> context.report("Done with task on " + ManagementFactory.getRuntimeMXBean().getName())
            );
            submitTasks(clusterContext, tasks);
        }
        executorService.shutdownNow();
    }

    private static void submitTasks(final DistributedClusterContext context,
        final Collection<LsClusterTask> tasks) throws IOException, InterruptedException {
        final InetSocketAddress address = context.nodes().iterator().next().getAddress();
        try (Socket slave = new Socket(address.getAddress(), address.getPort())) {
            try (ObjectOutputStream dataOut = new ObjectOutputStream(slave.getOutputStream())) {
                dataOut.writeObject(tasks);
                dataOut.flush();
                TimeUnit.SECONDS.sleep(10L);
            }
        }
    }

    private static final class LeaderService implements Callable<Void>, Closeable {

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private final ServerSocket serverSocket;

        LeaderService(final int masterPort) throws IOException {
            this.serverSocket = new ServerSocket(masterPort);
        }

        @Override
        public Void call() throws Exception {
            while (true) {
                System.out.println("Waiting for Slaves to report");
                try (Socket slave = serverSocket.accept()) {
                    System.out.println("Slave connected from " + slave.getInetAddress().getHostAddress());
                    final DataInputStream inputStream =
                        new DataInputStream(slave.getInputStream());
                    final String message = inputStream.readUTF();
                    System.out.println("Report: " + message);
                    final OutputStream outputStream = slave.getOutputStream();
                    outputStream.write(1);
                    outputStream.flush();
                    TimeUnit.SECONDS.sleep(2L);
                } catch (SocketException ex) {
                    if (closed.get() && !"Socket closed".equals(ex.getMessage())) {
                        throw new IllegalStateException(ex);
                    }
                    break;
                } catch (final IOException ex) {
                    System.err.println(ex);
                    throw new IllegalStateException(ex);
                }
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                serverSocket.close();
            }
        }
    }
}
