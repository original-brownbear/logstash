package org.logstash.cluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.logstash.LsClusterIntegTestCase;
import org.logstash.cluster.elasticsearch.EsClient;
import org.logstash.cluster.state.Partition;
import org.logstash.ext.EventQueue;
import org.logstash.ext.JrubyEventExtLibrary;
import org.logstash.plugins.generator.GeneratorClusterInput;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link ClusterInput}.
 */
public final class ClusterInputTest extends LsClusterIntegTestCase {

    public static void waitAllPartitionsAssigned(final EsClient client, final int count)
        throws InterruptedException {
        int waits = 1000;
        while (
            client.getPartitions().size() != count ||
                client.getPartitions().stream().anyMatch(partition -> partition.getOwner() != null
                    && partition.getOwner().isEmpty())
            ) {
            waits -= 1;
            TimeUnit.MILLISECONDS.sleep(100L);
            if (waits == 0) {
                Assert.fail();
            }
        }
    }

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
            final String local = client.getConfig().localNode();
            waitForNodeRegistration(client, local);
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
            waitForNodeRegistration(clientOne, nodeOne, nodeTwo);
            waitForNodeRegistration(clientTwo, nodeOne, nodeTwo);
            MatcherAssert.assertThat(
                clientOne.currentClusterNodes(), Matchers.containsInAnyOrder(nodeOne, nodeTwo)
            );
            MatcherAssert.assertThat(
                clientTwo.currentClusterNodes(), Matchers.containsInAnyOrder(nodeOne, nodeTwo)
            );
            waitAllPartitionsAssigned(clientOne, 2);
            final Collection<Partition> partitions = clientOne.getPartitions();
            MatcherAssert.assertThat(partitions.size(), is(2));
            MatcherAssert.assertThat(
                partitions.stream().filter(partition -> partition.getOwner().equals(nodeOne)).count(),
                is(1L)
            );
            MatcherAssert.assertThat(
                partitions.stream().filter(partition -> partition.getOwner().equals(nodeTwo)).count(),
                is(1L)
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
                    GeneratorClusterInput.class.getName()
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

    private static void waitForNodeRegistration(final EsClient client, final String... nodes)
        throws InterruptedException {
        int waits = 1000;
        while (!client.currentClusterNodes().containsAll(Arrays.asList(nodes))) {
            waits -= 1;
            TimeUnit.MILLISECONDS.sleep(100L);
            if (waits == 0) {
                Assert.fail();
            }
        }
    }
}
