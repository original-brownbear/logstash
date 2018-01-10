package org.logstash.cluster.elasticsearch;

import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.logstash.LsClusterIntegTestCase;
import org.logstash.cluster.LogstashClusterConfig;

import static org.hamcrest.Matchers.is;

public final class EsMapTest extends LsClusterIntegTestCase {

    @Test
    public void testContainsKey() throws Exception {
        try (EsClient client =
                 EsClient.create(
                     new LogstashClusterConfig("test", getClusterHosts())
                 )
        ) {
            final EsMap map = client.map("testMap");
            final String key1 = "foo";
            map.put(key1, "blub");
            MatcherAssert.assertThat(map.containsKey(key1), is(true));
            final String key2 = "bar";
            map.put(key2, "brrr");
            MatcherAssert.assertThat(map.containsKey(key2), is(true));
        }
    }
}
