package org.logstash.cluster;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.logstash.LsClusterIntegTestCase;
import org.logstash.RubyUtil;
import org.logstash.cluster.elasticsearch.EsClient;
import org.logstash.ext.EventQueue;
import org.logstash.ext.JrubyEventExtLibrary;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link ClusterInput}.
 */
public final class ClusterInputTest extends LsClusterIntegTestCase {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    public void testSimpleTask() throws Exception {
        final ExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        final String index = "testsimpletask";
        final LogstashClusterConfig config = new LogstashClusterConfig(
            "node1", index
        );
        try (EsClient configProvider = EsClient.create(client(), config)) {
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
                        "node2", index
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

        private final ClusterInput cluster;

        private final CountDownLatch stoppedLatch = new CountDownLatch(1);

        public SimpleTaskLeader(final ClusterInput cluster) {
            this.cluster = cluster;
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
            cluster.getTasks().pushTask(
                (server, events) -> events.push(
                    new JrubyEventExtLibrary.RubyEvent(RubyUtil.RUBY, RubyUtil.RUBY_EVENT_CLASS)
                )
            );
            stoppedLatch.countDown();
        }
    }
}
