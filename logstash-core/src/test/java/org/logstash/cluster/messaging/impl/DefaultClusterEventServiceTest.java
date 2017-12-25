package org.logstash.cluster.messaging.impl;

import com.google.common.util.concurrent.MoreExecutors;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.logstash.cluster.cluster.ClusterMetadata;
import org.logstash.cluster.cluster.ClusterService;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.cluster.impl.DefaultClusterService;
import org.logstash.cluster.messaging.ClusterCommunicationService;
import org.logstash.cluster.messaging.ClusterEventService;
import org.logstash.cluster.messaging.Endpoint;
import org.logstash.cluster.messaging.MessageSubject;
import org.logstash.cluster.messaging.MessagingService;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Cluster event service test.
 */
public final class DefaultClusterEventServiceTest {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.BASIC);

    @Test
    public void testClusterEventService() throws Exception {
        final TestMessagingServiceFactory factory = new TestMessagingServiceFactory();

        final ClusterMetadata clusterMetadata1 = buildClusterMetadata(1, 1, 2, 3);
        final MessagingService messagingService1 = factory.newMessagingService(clusterMetadata1.localNode().endpoint()).open().join();
        final ClusterService clusterService1 = new DefaultClusterService(clusterMetadata1, messagingService1).open().join();
        final ClusterCommunicationService clusterCommunicator1 = new DefaultClusterCommunicationService(clusterService1, messagingService1).open().join();
        final ClusterEventService eventService1 = new DefaultClusterEventService(clusterService1, clusterCommunicator1).open().join();

        final ClusterMetadata clusterMetadata2 = buildClusterMetadata(2, 1, 2, 3);
        final MessagingService messagingService2 = factory.newMessagingService(clusterMetadata2.localNode().endpoint()).open().join();
        final ClusterService clusterService2 = new DefaultClusterService(clusterMetadata2, factory.newMessagingService(clusterMetadata2.localNode().endpoint())).open().join();
        final ClusterCommunicationService clusterCommunicator2 = new DefaultClusterCommunicationService(clusterService2, messagingService2).open().join();
        final ClusterEventService eventService2 = new DefaultClusterEventService(clusterService2, clusterCommunicator2).open().join();

        final ClusterMetadata clusterMetadata3 = buildClusterMetadata(3, 1, 2, 3);
        final MessagingService messagingService3 = factory.newMessagingService(clusterMetadata3.localNode().endpoint()).open().join();
        final ClusterService clusterService3 = new DefaultClusterService(clusterMetadata3, messagingService3).open().join();
        final ClusterCommunicationService clusterCommunicator3 = new DefaultClusterCommunicationService(clusterService3, messagingService3).open().join();
        final ClusterEventService eventService3 = new DefaultClusterEventService(clusterService3, clusterCommunicator3).open().join();

        Thread.sleep(100L);

        final AtomicReference<String> value1 = new AtomicReference<>();
        eventService1.addSubscriber(new MessageSubject("test1"), SERIALIZER::decode, value1::set, MoreExecutors.directExecutor()).join();

        final AtomicReference<String> value2 = new AtomicReference<>();
        eventService2.addSubscriber(new MessageSubject("test1"), SERIALIZER::decode, value2::set, MoreExecutors.directExecutor()).join();

        eventService3.broadcast(new MessageSubject("test1"), "Hello world!", SERIALIZER::encode);

        Thread.sleep(100L);

        assertEquals("Hello world!", value1.get());
        assertEquals("Hello world!", value2.get());

        value1.set(null);
        value2.set(null);

        eventService3.unicast(new MessageSubject("test1"), "Hello world again!");
        Thread.sleep(100L);
        assertEquals("Hello world again!", value2.get());
        assertNull(value1.get());
        value2.set(null);

        eventService3.unicast(new MessageSubject("test1"), "Hello world again!");
        Thread.sleep(100L);
        assertEquals("Hello world again!", value1.get());
        assertNull(value2.get());
        value1.set(null);

        eventService3.unicast(new MessageSubject("test1"), "Hello world again!");
        Thread.sleep(100L);
        assertEquals("Hello world again!", value2.get());
        assertNull(value1.get());
        value2.set(null);

        eventService1.<String, String>addSubscriber(new MessageSubject("test2"), SERIALIZER::decode, message -> {
            value1.set(message);
            return message;
        }, SERIALIZER::encode, MoreExecutors.directExecutor());
        eventService2.<String, String>addSubscriber(new MessageSubject("test2"), SERIALIZER::decode, message -> {
            value2.set(message);
            return message;
        }, SERIALIZER::encode, MoreExecutors.directExecutor());

        assertEquals("Hello world!", eventService3.sendAndReceive(new MessageSubject("test2"), "Hello world!").join());
        assertEquals("Hello world!", value2.get());
        assertNull(value1.get());
        value2.set(null);

        assertEquals("Hello world!", eventService3.sendAndReceive(new MessageSubject("test2"), "Hello world!").join());
        assertEquals("Hello world!", value1.get());
        assertNull(value2.get());
        value1.set(null);

        assertEquals("Hello world!", eventService3.sendAndReceive(new MessageSubject("test2"), "Hello world!").join());
        assertEquals("Hello world!", value2.get());
        assertNull(value1.get());
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
