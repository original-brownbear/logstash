package org.logstash.cluster.elasticsearch;

import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.logstash.LsClusterIntegTestCase;
import org.logstash.cluster.LogstashClusterConfig;

/**
 * Tests for {@link EsClient}.
 */
public final class EsClientTest extends LsClusterIntegTestCase {

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
