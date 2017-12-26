package org.logstash.cluster;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.google.common.util.concurrent.Uninterruptibles;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.logstash.RubyUtil;
import org.logstash.TestUtils;
import org.logstash.cluster.primitives.queue.WorkQueue;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.ext.EventQueue;
import org.logstash.ext.JrubyEventExtLibrary;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link ClusterInput}.
 */
@ThreadLeakLingering(linger = 25000)
public final class ClusterInputTest extends ESIntegTestCase {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    public void testSimpleTask() throws Exception {
        ensureGreen();
        System.setSecurityManager(null);
        final ExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        final String index = "testsimpletask";
        final LogstashClusterConfig config = new LogstashClusterConfig(
            "node1",
            new InetSocketAddress(InetAddress.getLoopbackAddress(), TestUtils.freePort()),
            Collections.emptyList(), temporaryFolder.newFolder(), index
        );
        try (ClusterConfigProvider configProvider =
                 ClusterConfigProvider.esConfigProvider(client(), config)) {
            configProvider.publishJobSettings(
                Collections.singletonMap(
                    ClusterInput.LOGSTASH_TASK_CLASS_SETTING,
                    ClusterInputTest.SimpleTaskLeader.class.getName()
                )
            );
            final BlockingQueue<JrubyEventExtLibrary.RubyEvent> queue = new LinkedTransferQueue<>();
            try (ClusterInput input = new ClusterInput(EventQueue.wrap(queue), configProvider)) {
                exec.execute(input);
                final LogstashClusterServer cluster = LogstashClusterServer.fromConfig(
                    new LogstashClusterConfig(
                        "node2", new InetSocketAddress(InetAddress.getLoopbackAddress(),
                        TestUtils.freePort()), configProvider.currentClusterConfig().getBootstrap(),
                        temporaryFolder.newFolder(), index
                    )
                );
                MatcherAssert.assertThat(
                    cluster.getWorkQueueNames(), contains(ClusterInput.P2P_QUEUE_NAME)
                );
                MatcherAssert.assertThat(
                    queue.take(), instanceOf(JrubyEventExtLibrary.RubyEvent.class)
                );
                cluster.close().join();
            } finally {
                exec.shutdownNow();
            }
        }
    }

    public static final class SimpleTaskLeader implements ClusterInput.LeaderTask {

        private final LogstashClusterServer cluster;

        private final CountDownLatch stoppedLatch = new CountDownLatch(1);

        public SimpleTaskLeader(final ClusterInput cluster) {
            this.cluster = cluster.getCluster();
        }

        @Override
        public void awaitStop() {
            Uninterruptibles.awaitUninterruptibly(stoppedLatch);
        }

        @Override
        public void stop() {
        }

        @Override
        public void run() {
            try (final WorkQueue<EnqueueEvent> tasks =
                     cluster.<EnqueueEvent>workQueueBuilder().withName(ClusterInput.P2P_QUEUE_NAME)
                         .withSerializer(Serializer.JAVA).build()) {
                tasks.addOne(
                    (server, events) -> events.push(
                        new JrubyEventExtLibrary.RubyEvent(RubyUtil.RUBY, RubyUtil.RUBY_EVENT_CLASS)
                    )
                );
            } catch (final Exception ex) {
                throw new IllegalStateException(ex);
            }
            stoppedLatch.countDown();
        }
    }
}
