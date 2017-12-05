package org.logstash.cluster;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class ClusterNetworkClientServiceTest extends ESIntegTestCase {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void discoveryTest() throws Exception {
        final String index = "testIndex";
        ensureGreen();
        final InetSocketAddress listenAddrOne = TestUtil.randomLoopbackAddress();
        final InetSocketAddress listenAddrTwo = TestUtil.randomLoopbackAddress();
        final ExecutorService exec = Executors.newFixedThreadPool(2);
        try (
            ClusterStateService stateOne = new ClusterStateService(
                temp.newFolder().toPath().resolve("test.db").toFile(), client(), index
            );
            ClusterNetworkClientService clientOne = new ClusterNetworkClientService(stateOne, listenAddrOne);
            ClusterStateService stateTwo = new ClusterStateService(
                temp.newFolder().toPath().resolve("test.db").toFile(), client(), index
            );
            ClusterNetworkClientService clientTwo = new ClusterNetworkClientService(stateTwo, listenAddrTwo)) {
            exec.submit(clientOne);
            exec.submit(clientTwo);
            stateOne.registerPeer(listenAddrTwo);
            TimeUnit.SECONDS.sleep(3L);
            MatcherAssert.assertThat(
                stateTwo.peers().contains(listenAddrOne), Matchers.is(true)
            );
            MatcherAssert.assertThat(
                stateOne.peers().contains(listenAddrTwo), Matchers.is(true)
            );
        } finally {
            exec.shutdownNow();
            exec.awaitTermination(2L, TimeUnit.MINUTES);
        }
    }

}
