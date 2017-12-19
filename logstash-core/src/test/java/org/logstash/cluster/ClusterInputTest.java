package org.logstash.cluster;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.logstash.RubyUtil;
import org.logstash.TestUtils;
import org.logstash.cluster.primitives.queue.WorkQueue;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.ext.JrubyEventExtLibrary;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link ClusterInput}.
 */
public final class ClusterInputTest extends ESIntegTestCase {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    public void testSimpleTask() throws Exception {
        ensureGreen();
        System.setSecurityManager(null);
        final BlockingQueue<JrubyEventExtLibrary.RubyEvent> queue = new LinkedTransferQueue<>();
        final ExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        final String index = "testsimpletask";
        final LogstashClusterConfig config = new LogstashClusterConfig(
            "node1",
            new InetSocketAddress(InetAddress.getLoopbackAddress(), TestUtils.freePort()),
            Collections.emptyList(), temporaryFolder.newFolder(), index
        );
        try (
            ClusterConfigProvider configProvider =
                ClusterConfigProvider.esConfigProvider(client(), config);
            ClusterInput input = new ClusterInput(
                event -> {
                    try {
                        queue.put(event);
                    } catch (final InterruptedException ex) {
                        throw new IllegalStateException(ex);
                    }
                }, configProvider
            )
        ) {
            exec.execute(input);
            try {
                final LogstashClusterServer cluster = LogstashClusterServer.fromConfig(
                    new LogstashClusterConfig(
                        "node2", new InetSocketAddress(InetAddress.getLoopbackAddress(),
                        TestUtils.freePort()), configProvider.currentConfig().getBootstrap(),
                        temporaryFolder.newFolder(), index
                    )
                );
                MatcherAssert.assertThat(
                    cluster.getWorkQueueNames(), contains(ClusterInput.P2P_QUEUE_NAME)
                );
                final WorkQueue<EnqueueEvent> tasks =
                    cluster.<EnqueueEvent>workQueueBuilder().withName(ClusterInput.P2P_QUEUE_NAME)
                        .withSerializer(Serializer.JAVA).build();
                tasks.addOne(
                    events -> events.push(
                        new JrubyEventExtLibrary.RubyEvent(RubyUtil.RUBY, RubyUtil.RUBY_EVENT_CLASS)
                    )
                );
                MatcherAssert.assertThat(
                    queue.take(), instanceOf(JrubyEventExtLibrary.RubyEvent.class)
                );
                cluster.close().join();
            } finally {
                exec.shutdownNow();
                exec.awaitTermination(2L, TimeUnit.MINUTES);
            }
        }
    }
}
