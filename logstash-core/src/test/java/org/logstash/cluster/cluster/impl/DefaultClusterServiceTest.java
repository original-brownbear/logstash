package org.logstash.cluster.cluster.impl;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.logstash.cluster.cluster.ClusterMetadata;
import org.logstash.cluster.cluster.ClusterService;
import org.logstash.cluster.cluster.ManagedClusterService;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.Node.State;
import org.logstash.cluster.cluster.Node.Type;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.messaging.Endpoint;
import org.logstash.cluster.messaging.impl.TestMessagingServiceFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Default cluster service test.
 */
public class DefaultClusterServiceTest {

    @Test
    public void testClusterService() throws Exception {
        final TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();

        final ClusterMetadata clusterMetadata1 = buildClusterMetadata(1, 1, 2, 3);
        final ManagedClusterService clusterService1 = new DefaultClusterService(
            clusterMetadata1, messagingServiceFactory.newMessagingService(clusterMetadata1.localNode().endpoint()).open().join());
        final ClusterMetadata clusterMetadata2 = buildClusterMetadata(2, 1, 2, 3);
        final ManagedClusterService clusterService2 = new DefaultClusterService(
            clusterMetadata2, messagingServiceFactory.newMessagingService(clusterMetadata2.localNode().endpoint()).open().join());
        final ClusterMetadata clusterMetadata3 = buildClusterMetadata(3, 1, 2, 3);
        final ManagedClusterService clusterService3 = new DefaultClusterService(
            clusterMetadata3, messagingServiceFactory.newMessagingService(clusterMetadata3.localNode().endpoint()).open().join());

        assertEquals(State.INACTIVE, clusterService1.getNode(NodeId.from("1")).state());
        assertEquals(State.INACTIVE, clusterService1.getNode(NodeId.from("2")).state());
        assertEquals(State.INACTIVE, clusterService1.getNode(NodeId.from("3")).state());

        final CompletableFuture<ClusterService>[] futures = new CompletableFuture[3];
        futures[0] = clusterService1.open();
        futures[1] = clusterService2.open();
        futures[2] = clusterService3.open();

        CompletableFuture.allOf(futures).join();

        Thread.sleep(1000L);

        assertEquals(3L, (long) clusterService1.getNodes().size());
        assertEquals(3L, (long) clusterService2.getNodes().size());
        assertEquals(3L, (long) clusterService3.getNodes().size());

        assertEquals(Type.CORE, clusterService1.getLocalNode().type());
        assertEquals(Type.CORE, clusterService1.getNode(NodeId.from("1")).type());
        assertEquals(Type.CORE, clusterService1.getNode(NodeId.from("2")).type());
        assertEquals(Type.CORE, clusterService1.getNode(NodeId.from("3")).type());

        assertEquals(State.ACTIVE, clusterService1.getLocalNode().state());
        assertEquals(State.ACTIVE, clusterService1.getNode(NodeId.from("1")).state());
        assertEquals(State.ACTIVE, clusterService1.getNode(NodeId.from("2")).state());
        assertEquals(State.ACTIVE, clusterService1.getNode(NodeId.from("3")).state());

        final ClusterMetadata clientMetadata = buildClusterMetadata(4, 1, 2, 3);

        final ManagedClusterService clientClusterService = new DefaultClusterService(
            clientMetadata, messagingServiceFactory.newMessagingService(clientMetadata.localNode().endpoint()).open().join());

        assertEquals(State.INACTIVE, clientClusterService.getLocalNode().state());

        assertEquals(State.INACTIVE, clientClusterService.getNode(NodeId.from("1")).state());
        assertEquals(State.INACTIVE, clientClusterService.getNode(NodeId.from("2")).state());
        assertEquals(State.INACTIVE, clientClusterService.getNode(NodeId.from("3")).state());
        assertEquals(State.INACTIVE, clientClusterService.getNode(NodeId.from("4")).state());

        clientClusterService.open().join();

        Thread.sleep(100L);

        assertEquals(4L, (long) clusterService1.getNodes().size());
        assertEquals(4L, (long) clusterService2.getNodes().size());
        assertEquals(4L, (long) clusterService3.getNodes().size());
        assertEquals(4L, (long) clientClusterService.getNodes().size());

        assertEquals(Type.CLIENT, clientClusterService.getLocalNode().type());

        assertEquals(Type.CORE, clientClusterService.getNode(NodeId.from("1")).type());
        assertEquals(Type.CORE, clientClusterService.getNode(NodeId.from("2")).type());
        assertEquals(Type.CORE, clientClusterService.getNode(NodeId.from("3")).type());
        assertEquals(Type.CLIENT, clientClusterService.getNode(NodeId.from("4")).type());

        assertEquals(State.ACTIVE, clientClusterService.getLocalNode().state());

        assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("1")).state());
        assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("2")).state());
        assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("3")).state());
        assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("4")).state());

        Thread.sleep(2500L);

        clusterService1.close().join();

        Thread.sleep(2500L);

        assertEquals(4L, (long) clusterService2.getNodes().size());
        assertEquals(Type.CORE, clusterService2.getNode(NodeId.from("1")).type());

        assertEquals(State.INACTIVE, clusterService2.getNode(NodeId.from("1")).state());
        assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("2")).state());
        assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("3")).state());
        assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("4")).state());

        assertEquals(State.INACTIVE, clientClusterService.getNode(NodeId.from("1")).state());
        assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("2")).state());
        assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("3")).state());
        assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("4")).state());

        clientClusterService.close().join();

        Thread.sleep(2500L);

        assertEquals(3L, (long) clusterService2.getNodes().size());

        assertEquals(State.INACTIVE, clusterService2.getNode(NodeId.from("1")).state());
        assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("2")).state());
        assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("3")).state());
        assertNull(clusterService2.getNode(NodeId.from("4")));
    }

    private static ClusterMetadata buildClusterMetadata(final int nodeId, final int... bootstrapNodes) {
        final InetAddress loopback = InetAddress.getLoopbackAddress();
        final ClusterMetadata.Builder metadataBuilder = ClusterMetadata.builder()
            .withLocalNode(Node.builder()
                .withId(NodeId.from(String.valueOf(nodeId)))
                .withEndpoint(new Endpoint(loopback, nodeId))
                .build());
        final List<Node> bootstrap = new ArrayList<>();
        for (final int bootstrapNode : bootstrapNodes) {
            bootstrap.add(Node.builder()
                .withId(NodeId.from(String.valueOf(bootstrapNode)))
                .withEndpoint(new Endpoint(loopback, bootstrapNode))
                .build());
        }
        return metadataBuilder.withBootstrapNodes(bootstrap).build();
    }
}
