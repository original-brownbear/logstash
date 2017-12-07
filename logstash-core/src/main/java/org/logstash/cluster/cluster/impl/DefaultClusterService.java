package org.logstash.cluster.cluster.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.logstash.cluster.cluster.ClusterEvent;
import org.logstash.cluster.cluster.ClusterEventListener;
import org.logstash.cluster.cluster.ClusterMetadata;
import org.logstash.cluster.cluster.ClusterService;
import org.logstash.cluster.cluster.ManagedClusterService;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.messaging.Endpoint;
import org.logstash.cluster.messaging.MessagingService;
import org.logstash.cluster.protocols.phi.PhiAccrualFailureDetector;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.utils.concurrent.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default cluster implementation.
 */
public class DefaultClusterService implements ManagedClusterService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusterService.class);

    private static final int DEFAULT_HEARTBEAT_INTERVAL = 100;
    private static final int DEFAULT_PHI_FAILURE_THRESHOLD = 10;
    private static final String HEARTBEAT_MESSAGE = "onos-cluster-heartbeat";
    private static final Serializer SERIALIZER = Serializer.using(
        KryoNamespace.builder()
            .register(KryoNamespaces.BASIC)
            .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
            .register(NodeId.class)
            .build("ClusterStore"));
    private final MessagingService messagingService;
    private final AtomicBoolean open = new AtomicBoolean();
    private final DefaultNode localNode;
    private final Map<NodeId, DefaultNode> nodes = Maps.newConcurrentMap();
    private final Map<NodeId, PhiAccrualFailureDetector> failureDetectors = Maps.newConcurrentMap();
    private final Set<ClusterEventListener> eventListeners = Sets.newCopyOnWriteArraySet();
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
        Threads.namedThreads("atomix-cluster-heartbeat-sender", LOGGER));
    private final ExecutorService heartbeatExecutor = Executors.newSingleThreadExecutor(
        Threads.namedThreads("atomix-cluster-heartbeat-receiver", LOGGER));
    private int heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private int phiFailureThreshold = DEFAULT_PHI_FAILURE_THRESHOLD;
    private ScheduledFuture<?> heartbeatFuture;

    public DefaultClusterService(ClusterMetadata clusterMetadata, MessagingService messagingService) {
        this.messagingService = Preconditions.checkNotNull(messagingService, "messagingService cannot be null");
        this.localNode = (DefaultNode) clusterMetadata.localNode();
        if (clusterMetadata.bootstrapNodes().contains(localNode)) {
            localNode.setType(Node.Type.CORE);
        } else {
            localNode.setType(Node.Type.CLIENT);
        }
        nodes.put(localNode.id(), localNode);
        clusterMetadata.bootstrapNodes().forEach(n -> nodes.putIfAbsent(n.id(), ((DefaultNode) n).setType(Node.Type.CORE)));
        messagingService.registerHandler(HEARTBEAT_MESSAGE, this::handleHeartbeat, heartbeatExecutor);
    }

    /**
     * Sends heartbeats to all peers.
     */
    private void sendHeartbeats() {
        try {
            Set<DefaultNode> peers = nodes.values()
                .stream()
                .filter(node -> !node.id().equals(getLocalNode().id()))
                .collect(Collectors.toSet());
            byte[] payload = SERIALIZER.encode(localNode.id());
            peers.forEach((node) -> {
                sendHeartbeat(node.endpoint(), payload);
                double phi = failureDetectors.computeIfAbsent(node.id(), n -> new PhiAccrualFailureDetector()).phi();
                if (phi >= phiFailureThreshold) {
                    if (node.state() == Node.State.ACTIVE) {
                        deactivateNode(node);
                    }
                } else {
                    if (node.state() == Node.State.INACTIVE) {
                        activateNode(node);
                    }
                }
            });
        } catch (Exception e) {
            LOGGER.debug("Failed to send heartbeat", e);
        }
    }

    @Override
    public Node getLocalNode() {
        return localNode;
    }

    @Override
    public Set<Node> getNodes() {
        return ImmutableSet.copyOf(nodes.values());
    }

    @Override
    public Node getNode(NodeId nodeId) {
        return nodes.get(nodeId);
    }

    /**
     * Sends a heartbeat to the given peer.
     */
    private void sendHeartbeat(Endpoint endpoint, byte[] payload) {
        messagingService.sendAsync(endpoint, HEARTBEAT_MESSAGE, payload).whenComplete((result, error) -> {
            if (error != null) {
                LOGGER.trace("Sending heartbeat to {} failed", endpoint, error);
            }
        });
    }

    /**
     * Activates the given node.
     */
    private void activateNode(DefaultNode node) {
        DefaultNode existingNode = nodes.get(node.id());
        if (existingNode == null) {
            node.setState(Node.State.ACTIVE);
            nodes.put(node.id(), node);
            eventListeners.forEach(l -> l.onEvent(new ClusterEvent(ClusterEvent.Type.NODE_ADDED, node)));
            sendHeartbeat(node.endpoint(), SERIALIZER.encode(localNode.id()));
        } else if (existingNode.state() == Node.State.INACTIVE) {
            existingNode.setState(Node.State.ACTIVE);
            eventListeners.forEach(l -> l.onEvent(new ClusterEvent(ClusterEvent.Type.NODE_ACTIVATED, existingNode)));
        }
    }

    /**
     * Deactivates the given node.
     */
    private void deactivateNode(DefaultNode node) {
        DefaultNode existingNode = nodes.get(node.id());
        if (existingNode != null && existingNode.state() == Node.State.ACTIVE) {
            existingNode.setState(Node.State.INACTIVE);
            switch (existingNode.type()) {
                case CORE:
                    eventListeners.forEach(l -> l.onEvent(new ClusterEvent(ClusterEvent.Type.NODE_DEACTIVATED, existingNode)));
                    break;
                case CLIENT:
                    nodes.remove(node.id());
                    eventListeners.forEach(l -> l.onEvent(new ClusterEvent(ClusterEvent.Type.NODE_REMOVED, existingNode)));
                    break;
                default:
                    throw new AssertionError();
            }
        }
    }

    /**
     * Handles a heartbeat message.
     */
    private void handleHeartbeat(Endpoint endpoint, byte[] message) {
        NodeId nodeId = SERIALIZER.decode(message);
        failureDetectors.computeIfAbsent(nodeId, n -> new PhiAccrualFailureDetector()).report();
        activateNode(new DefaultNode(nodeId, endpoint));
    }

    @Override
    public void addListener(ClusterEventListener listener) {
        eventListeners.add(listener);
    }

    @Override
    public void removeListener(ClusterEventListener listener) {
        eventListeners.remove(listener);
    }

    @Override
    public CompletableFuture<ClusterService> open() {
        if (open.compareAndSet(false, true)) {
            localNode.setState(Node.State.ACTIVE);
            heartbeatFuture = heartbeatScheduler.scheduleWithFixedDelay(this::sendHeartbeats, 0, heartbeatInterval, TimeUnit.MILLISECONDS);
        }
        LOGGER.info("Started");
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public CompletableFuture<Void> close() {
        if (open.compareAndSet(true, false)) {
            localNode.setState(Node.State.INACTIVE);
            heartbeatFuture.cancel(true);
        }
        LOGGER.info("Stopped");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isClosed() {
        return !open.get();
    }
}
