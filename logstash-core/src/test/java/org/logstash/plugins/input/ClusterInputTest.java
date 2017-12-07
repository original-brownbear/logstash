package org.logstash.plugins.input;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.TestUtils;
import org.logstash.cluster.LogstashClusterConfig;
import org.logstash.ext.JrubyEventExtLibrary;

/**
 * Tests for {@link ClusterInput}.
 */
public final class ClusterInputTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testSimpleTask() throws Exception {
        final BlockingQueue<JrubyEventExtLibrary.RubyEvent> queue = new ArrayBlockingQueue<>(1);
        final ExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        final LogstashClusterConfig config = new LogstashClusterConfig(
            new InetSocketAddress(InetAddress.getLoopbackAddress(), TestUtils.freePort()),
            Collections.emptyList(), temporaryFolder.newFolder()
        );
        try (final ClusterInput input =
                 new ClusterInput(
                     event -> {
                         try {
                             queue.put(event);
                         } catch (final InterruptedException ex) {
                             throw new IllegalStateException(ex);
                         }
                     },
                     config
                 )
        ) {
            exec.execute(input);
        }
    }
}
