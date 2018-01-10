package org.logstash.cluster.elasticsearch;

import java.util.Collections;
import java.util.UUID;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.LsClusterIntegTestCase;
import org.logstash.cluster.LogstashClusterConfig;

/**
 * Tests for {@link EsClient}.
 */
public final class EsClientTest extends LsClusterIntegTestCase {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void clusterBootstrapTest() throws Exception {
        final String local = UUID.randomUUID().toString();
        try (
            EsClient client = EsClient.create(
                new LogstashClusterConfig("clusterbootstraptest", getClusterHosts())
            )
        ) {
            MatcherAssert.assertThat(client.currentClusterNodes(), Matchers.contains(local));
        }
    }

    @Test
    public void multipleNodesTest() throws Exception {
        final String index = "multiplenodestest";
        try (EsClient clientOne = EsClient.create(new LogstashClusterConfig(index, getClusterHosts()))) {
            try (EsClient clientTwo = EsClient.create(new LogstashClusterConfig(index, getClusterHosts()))) {
                final String nodeOne = clientOne.getConfig().localNode();
                final String nodeTwo = UUID.randomUUID().toString();
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
        try (EsClient client =
                 EsClient.create(
                     new LogstashClusterConfig(
                         "getandupdatesettings", getClusterHosts()
                     )
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
