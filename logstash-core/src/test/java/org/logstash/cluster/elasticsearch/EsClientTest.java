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
        try (EsClient client = new EsClient(client(), "clusterbootstraptest", local)) {
            final Collection<Node> initial = client.loadBootstrap();
            MatcherAssert.assertThat(initial, Matchers.contains(local));
        }
    }
}
