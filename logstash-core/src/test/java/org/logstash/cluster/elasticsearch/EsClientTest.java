package org.logstash.cluster.elasticsearch;

import java.net.InetAddress;
import java.util.Collections;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.TestUtils;
import org.logstash.cluster.ClusterConfigProvider;
import org.logstash.cluster.LogstashClusterConfig;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.cluster.impl.DefaultNode;
import org.logstash.cluster.messaging.Endpoint;

/**
 * Tests for {@link EsClient}.
 */
public final class EsClientTest extends ESIntegTestCase {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void clusterBootstrapTest() throws Exception {
        ensureGreen();
        final Node local = new DefaultNode(
            NodeId.from("someId"),
            new Endpoint(InetAddress.getLoopbackAddress(), TestUtils.freePort())
        );
        try (ClusterConfigProvider client =
                 ClusterConfigProvider.esConfigProvider(
                     client(),
                     new LogstashClusterConfig(
                         local, Collections.emptyList(), temporaryFolder.newFolder(),
                         "clusterbootstraptest"
                     )
                 )
        ) {
            MatcherAssert.assertThat(client.currentClusterConfig().getBootstrap(), Matchers.contains(local));
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
        try (ClusterConfigProvider clientOne =
                 ClusterConfigProvider.esConfigProvider(
                     client(),
                     new LogstashClusterConfig(
                         nodeOne, Collections.emptyList(), temporaryFolder.newFolder(), index
                     )
                 )
        ) {
            final Node nodeTwo = new DefaultNode(
                NodeId.from("id-two"),
                new Endpoint(InetAddress.getLoopbackAddress(), TestUtils.freePort())
            );
            try (ClusterConfigProvider clientTwo =
                     ClusterConfigProvider.esConfigProvider(
                         client(), new LogstashClusterConfig(
                             nodeTwo, Collections.emptyList(), temporaryFolder.newFolder(), index
                         )
                     )
            ) {
                MatcherAssert.assertThat(
                    clientOne.currentClusterConfig().getBootstrap(), Matchers.contains(nodeOne, nodeTwo)
                );
                MatcherAssert.assertThat(
                    clientTwo.currentClusterConfig().getBootstrap(), Matchers.contains(nodeOne, nodeTwo)
                );
            }
        }
    }
}
