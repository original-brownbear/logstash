package org.logstash.cluster;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.logstash.LsClusterIntegTestCase;
import org.logstash.RubyUtil;
import org.logstash.cluster.elasticsearch.EsClient;
import org.logstash.ext.EventQueue;
import org.logstash.ext.JrubyEventExtLibrary;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link ClusterInput}.
 */
public final class ClusterInputTest extends LsClusterIntegTestCase {

    @Test
    public void clusterBootstrapTest() throws Exception {
        final ExecutorService exec = Executors.newSingleThreadExecutor();
        try (
            EsClient client = EsClient.create(
                new LogstashClusterConfig("clusterbootstraptest", getClusterHosts())
            );
            ClusterInput input = new ClusterInput(
                EventQueue.wrap(new ArrayBlockingQueue<>(1)), client
            )
        ) {
            exec.submit(input);
            int waits = 100;
            final String local = client.getConfig().localNode();
            while (!client.currentClusterNodes().contains(local) && waits > 0) {
                waits -= 1;
                TimeUnit.MILLISECONDS.sleep(100L);
            }
            MatcherAssert.assertThat(client.currentClusterNodes(), Matchers.contains(local));
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    public void multipleNodesTest() throws Exception {
        final String index = "multiplenodestest";
        final ExecutorService exec = Executors.newFixedThreadPool(2);
        try (EsClient clientOne = EsClient.create(new LogstashClusterConfig(index, getClusterHosts()));
             EsClient clientTwo = EsClient.create(new LogstashClusterConfig(index, getClusterHosts()));
             ClusterInput inputOne = new ClusterInput(
                 EventQueue.wrap(new ArrayBlockingQueue<>(1)), clientOne
             );
             ClusterInput inputTwo = new ClusterInput(
                 EventQueue.wrap(new ArrayBlockingQueue<>(1)), clientTwo
             )
        ) {
            exec.submit(inputOne);
            exec.submit(inputTwo);
            final String nodeOne = clientOne.getConfig().localNode();
            final String nodeTwo = clientTwo.getConfig().localNode();
            int waits = 1000;
            while (!clientOne.currentClusterNodes().containsAll(Arrays.asList(nodeOne, nodeTwo))) {
                waits -= 1;
                TimeUnit.MILLISECONDS.sleep(100L);
                if (waits == 0) {
                    Assert.fail();
                }
            }
            waits = 1000;
            while (!clientTwo.currentClusterNodes().containsAll(Arrays.asList(nodeOne, nodeTwo))) {
                waits -= 1;
                TimeUnit.MILLISECONDS.sleep(100L);
                if (waits == 0) {
                    Assert.fail();
                }
            }
            MatcherAssert.assertThat(
                clientOne.currentClusterNodes(), Matchers.containsInAnyOrder(nodeOne, nodeTwo)
            );
            MatcherAssert.assertThat(
                clientTwo.currentClusterNodes(), Matchers.containsInAnyOrder(nodeOne, nodeTwo)
            );
        } finally {
            exec.shutdownNow();
        }
    }

    public void testSimpleTask() throws Exception {
        final ExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        final String index = "testsimpletask";
        final LogstashClusterConfig config = new LogstashClusterConfig(index, getClusterHosts());
        try (EsClient esClient = EsClient.create(config)) {
            esClient.publishJobSettings(
                Collections.singletonMap(
                    ClusterInput.LOGSTASH_TASK_CLASS_SETTING,
                    ClusterInputTest.SimpleTaskLeader.class.getName()
                )
            );
            final BlockingQueue<JrubyEventExtLibrary.RubyEvent> queue = new LinkedTransferQueue<>();
            try (ClusterInput input = new ClusterInput(EventQueue.wrap(queue), esClient)) {
                exec.execute(input);
                MatcherAssert.assertThat(
                    queue.take(), instanceOf(JrubyEventExtLibrary.RubyEvent.class)
                );
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
