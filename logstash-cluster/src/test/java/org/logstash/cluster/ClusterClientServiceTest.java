package org.logstash.cluster;

import java.net.InetSocketAddress;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class ClusterClientServiceTest extends ESIntegTestCase {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void discoveryTest() throws Exception {
        final String index = "testIndex";
        ensureGreen();
        final InetSocketAddress listenAddrOne = new InetSocketAddress(PortUtil.reserve());
        final InetSocketAddress listenAddrTwo = new InetSocketAddress(PortUtil.reserve());
        try (
            ClusterStateManagerService state = new ClusterStateManagerService(
                temp.newFolder().toPath().resolve("test.db").toFile(), client(), index
            );
            ClusterClientService client = new ClusterClientService(state, listenAddrOne)) {
            try (
                ClusterStateManagerService stateTwo = new ClusterStateManagerService(
                    temp.newFolder().toPath().resolve("test.db").toFile(), client(), index
                );
                ClusterClientService clientTwo = new ClusterClientService(stateTwo, listenAddrTwo)) {

            }
        }
    }

}
