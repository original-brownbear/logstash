package org.logstash.plugins.input;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.RubyUtil;
import org.logstash.TestUtils;
import org.logstash.cluster.LogstashCluster;
import org.logstash.cluster.LogstashClusterConfig;
import org.logstash.cluster.primitives.queue.WorkQueue;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.ext.JrubyEventExtLibrary;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

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
            LogstashCluster cluster = null;
            try {
                cluster = getClient(
                    new LogstashClusterConfig(
                        new InetSocketAddress(
                            InetAddress.getLoopbackAddress(),
                            TestUtils.freePort()
                        ),
                        Collections.singleton(config.localNode()),
                        temporaryFolder.newFolder()
                    )
                );
                assertThat(cluster.getWorkQueueNames(), contains("logstashWorkQueue"));
                final WorkQueue<EnqueueEvent> tasks =
                    cluster.<EnqueueEvent>workQueueBuilder().withName("logstashWorkQueue")
                        .withSerializer(Serializer.JAVA).build();
                tasks.addOne(
                    events -> events.push(
                        new JrubyEventExtLibrary.RubyEvent(RubyUtil.RUBY, RubyUtil.RUBY_EVENT_CLASS)
                    )
                );
                assertThat(
                    queue.poll(10L, TimeUnit.SECONDS),
                    instanceOf(JrubyEventExtLibrary.RubyEvent.class)
                );
            } finally {
                if (cluster != null) {
                    cluster.close().join();
                }
            }
        }
    }

    private static LogstashCluster getClient(final LogstashClusterConfig config) {
        return LogstashCluster.builder().withLocalNode(config.localNode())
            .withBootstrapNodes(config.getBootstrap())
            .withDataDir(config.getDataDir())
            .withNumPartitions(1)
            .build()
            .open().join();
    }
}
