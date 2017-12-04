package org.logstash.cluster;

import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class ClusterClientServiceTest extends ESIntegTestCase {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void discoveryTest() throws Exception {
        ensureGreen();
        try (
            ClusterStateManagerService state = new ClusterStateManagerService(
                temp.newFolder().toPath().resolve("test.db").toFile(), client()
            );
            ClusterClientService client = new ClusterClientService(state)) {

        }
    }
}
