package org.logstash.cluster.elasticsearch;

import java.net.InetAddress;
import java.util.Collection;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.logstash.TestUtils;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.cluster.impl.DefaultNode;
import org.logstash.cluster.messaging.Endpoint;

/**
 * Tests for {@link EsClient}.
 */
public final class EsClientTest extends ESIntegTestCase {

    @Test
    public void clusterBootstrapTest() throws Exception {
        ensureGreen();
        final Node local = new DefaultNode(
            NodeId.from("someId"),
            new Endpoint(InetAddress.getLoopbackAddress(), TestUtils.freePort())
        );
        try (EsClient client = EsClient.create(client(), "clusterbootstraptest", local)) {
            final Collection<Node> initial = client.loadBootstrap();
            MatcherAssert.assertThat(initial, Matchers.contains(local));
        }
    }

    @Test
    public void multipleNodesTest() throws Exception {
        ensureGreen();
        final Node nodeOne = new DefaultNode(
            NodeId.from("id-one"), new Endpoint(InetAddress.getLoopbackAddress(),
            TestUtils.freePort())
        );
        final String index = "multiplenodestest";
        try (final EsClient clientOne = EsClient.create(client(), index, nodeOne)) {
            final Node nodeTwo = new DefaultNode(
                NodeId.from("id-two"),
                new Endpoint(InetAddress.getLoopbackAddress(), TestUtils.freePort())
            );
            try (EsClient clientTwo = EsClient.create(client(), index, nodeTwo)) {
                MatcherAssert.assertThat(
                    clientOne.loadBootstrap(), Matchers.contains(nodeOne, nodeTwo)
                );
                MatcherAssert.assertThat(
                    clientTwo.loadBootstrap(), Matchers.contains(nodeOne, nodeTwo)
                );
            }
        }
    }
}
