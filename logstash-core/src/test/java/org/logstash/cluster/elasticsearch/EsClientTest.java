package org.logstash.cluster.elasticsearch;

import java.net.InetAddress;
import java.util.Collections;
import java.util.UUID;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.LsClusterIntegTestCase;
import org.logstash.TestUtils;
import org.logstash.cluster.LogstashClusterConfig;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.cluster.impl.DefaultNode;
import org.logstash.cluster.messaging.Endpoint;

/**
 * Tests for {@link EsClient}.
 */
public final class EsClientTest extends LsClusterIntegTestCase {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void clusterBootstrapTest() throws Exception {
        final Node local = new DefaultNode(
            NodeId.from("someId"),
            new Endpoint(InetAddress.getLoopbackAddress(), TestUtils.freePort())
        );
        try (EsClient client = EsClient.create(
            client(),
            new LogstashClusterConfig("clusterbootstraptest")
        )
        ) {
            MatcherAssert.assertThat(client.currentClusterNodes(), Matchers.contains(local));
        }
    }

    @Test
    public void multipleNodesTest() throws Exception {
        final String nodeOne = UUID.randomUUID().toString();
        final String index = "multiplenodestest";
        try (EsClient clientOne =
                 EsClient.create(
                     client(),
                     new LogstashClusterConfig(
                         nodeOne, index
                     )
                 )
        ) {
            final String nodeTwo = UUID.randomUUID().toString();
            try (EsClient clientTwo =
                     EsClient.create(
                         client(), new LogstashClusterConfig(nodeTwo, index)
                     )
            ) {
                MatcherAssert.assertThat(
                    clientOne.currentClusterNodes(), Matchers.contains(nodeOne, nodeTwo)
                );
                MatcherAssert.assertThat(
                    clientTwo.currentClusterNodes(), Matchers.contains(nodeOne, nodeTwo)
                );
            }
        }
    }

    @Test
    public void getAndUpdateSettings() throws Exception {
        final String local = UUID.randomUUID().toString();
        try (EsClient client =
                 EsClient.create(
                     client(), new LogstashClusterConfig(local, "getandupdatesettings")
                 )
        ) {
            MatcherAssert.assertThat(client.currentJobSettings(), Matchers.is(Collections.emptyMap()));
            final String key = "foo";
            final String value = "bar";
            client.publishJobSettings(Collections.singletonMap(key, value));
            MatcherAssert.assertThat(client.currentJobSettings().get(key), Matchers.is(value));
        }
    }
}
