package org.logstash.cluster.messaging.impl;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.logstash.cluster.cluster.ClusterService;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.messaging.ClusterCommunicationService;
import org.logstash.cluster.messaging.ClusterEventService;
import org.logstash.cluster.messaging.ManagedClusterEventService;
import org.logstash.cluster.messaging.MessageSubject;
import org.logstash.cluster.messaging.MessagingException;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.time.LogicalTimestamp;
import org.logstash.cluster.time.WallClockTimestamp;
import org.logstash.cluster.utils.concurrent.Futures;
import org.logstash.cluster.utils.concurrent.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster event service.
 */
public class DefaultClusterEventService implements ManagedClusterEventService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusterEventService.class);

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(NodeId.class)
        .register(Subscription.class)
        .register(MessageSubject.class)
        .register(LogicalTimestamp.class)
        .register(WallClockTimestamp.class)
        .build());

    private static final MessageSubject GOSSIP_MESSAGE_SUBJECT = new MessageSubject("ClusterEventService-update");

    private static final long GOSSIP_INTERVAL_MILLIS = 1000;
    private static final long TOMBSTONE_EXPIRATION_MILLIS = 1000 * 60;

    private final ClusterService clusterService;
    private final ClusterCommunicationService clusterCommunicator;
    private final NodeId localNodeId;
    private final AtomicLong logicalTime = new AtomicLong();
    private final Map<NodeId, Long> updateTimes = Maps.newConcurrentMap();
    private final Map<MessageSubject, Map<NodeId, Subscription>> subjectSubscriptions = Maps.newConcurrentMap();
    private final Map<MessageSubject, SubscriberIterator> subjectIterators = Maps.newConcurrentMap();
    private final AtomicBoolean open = new AtomicBoolean();
    private ScheduledExecutorService gossipExecutor;

    public DefaultClusterEventService(ClusterService clusterService, ClusterCommunicationService clusterCommunicator) {
        this.clusterService = clusterService;
        this.clusterCommunicator = clusterCommunicator;
        this.localNodeId = clusterService.getLocalNode().id();
    }

    @Override
    public <M> void broadcast(MessageSubject subject, M message, Function<M, byte[]> encoder) {
        Collection<? extends NodeId> subscribers = getSubscriberNodes(subject);
        if (subscribers != null) {
            subscribers.forEach(nodeId -> clusterCommunicator.unicast(subject, message, encoder, nodeId));
        }
    }

    @Override
    public <M> CompletableFuture<Void> unicast(MessageSubject subject, M message, Function<M, byte[]> encoder) {
        NodeId nodeId = getNextNodeId(subject);
        if (nodeId != null) {
            return clusterCommunicator.unicast(subject, message, encoder, nodeId);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public <M, R> CompletableFuture<R> sendAndReceive(MessageSubject subject, M message, Function<M, byte[]> encoder, Function<byte[], R> decoder) {
        NodeId nodeId = getNextNodeId(subject);
        if (nodeId == null) {
            return Futures.exceptionalFuture(new MessagingException.NoRemoteHandler());
        }
        return clusterCommunicator.sendAndReceive(subject, message, encoder, decoder, nodeId);
    }

    @Override
    public <M, R> CompletableFuture<Void> addSubscriber(MessageSubject subject, Function<byte[], M> decoder, Function<M, R> handler, Function<R, byte[]> encoder, Executor executor) {
        return clusterCommunicator.addSubscriber(subject, decoder, handler, encoder, executor)
            .thenCompose(v -> registerSubscriber(subject));
    }

    /**
     * Registers the node as a subscriber for the given subject.
     * @param subject the subject for which to register the node as a subscriber
     */
    private synchronized CompletableFuture<Void> registerSubscriber(MessageSubject subject) {
        Map<NodeId, Subscription> nodeSubscriptions =
            subjectSubscriptions.computeIfAbsent(subject, s -> Maps.newConcurrentMap());
        Subscription subscription = new Subscription(
            localNodeId,
            subject,
            new LogicalTimestamp(logicalTime.incrementAndGet()));
        nodeSubscriptions.put(localNodeId, subscription);
        return updateNodes();
    }

    /**
     * Updates all active peers with a given subscription.
     */
    private CompletableFuture<Void> updateNodes() {
        List<CompletableFuture<Void>> futures = clusterService.getNodes()
            .stream()
            .filter(node -> !localNodeId.equals(node.id()))
            .map(Node::id)
            .map(this::updateNode)
            .collect(Collectors.toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }

    @Override
    public <M, R> CompletableFuture<Void> addSubscriber(MessageSubject subject, Function<byte[], M> decoder, Function<M, CompletableFuture<R>> handler, Function<R, byte[]> encoder) {
        registerSubscriber(subject);
        return clusterCommunicator.addSubscriber(subject, decoder, handler, encoder)
            .thenCompose(v -> registerSubscriber(subject));
    }

    @Override
    public <M> CompletableFuture<Void> addSubscriber(MessageSubject subject, Function<byte[], M> decoder, Consumer<M> handler, Executor executor) {
        return clusterCommunicator.addSubscriber(subject, decoder, handler, executor)
            .thenCompose(v -> registerSubscriber(subject));
    }

    @Override
    public void removeSubscriber(MessageSubject subject) {
        unregisterSubscriber(subject);
        clusterCommunicator.removeSubscriber(subject);
    }

    /**
     * Unregisters the node as a subscriber for the given subject.
     * @param subject the subject for which to unregister the node as a subscriber
     */
    private synchronized void unregisterSubscriber(MessageSubject subject) {
        Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.get(subject);
        if (nodeSubscriptions != null) {
            Subscription subscription = nodeSubscriptions.get(localNodeId);
            if (subscription != null) {
                nodeSubscriptions.put(localNodeId, subscription.asTombstone());
                updateNodes();
            }
        }
    }

    /**
     * Returns the next node ID for the given message subject.
     * @param subject the subject for which to return the next node ID
     * @return the next node ID for the given message subject
     */
    private NodeId getNextNodeId(MessageSubject subject) {
        SubscriberIterator iterator = subjectIterators.get(subject);
        return iterator != null && iterator.hasNext() ? iterator.next() : null;
    }

    /**
     * Returns the set of nodes with subscribers for the given message subject.
     * @param subject the subject for which to return a set of nodes
     * @return a set of nodes with subscribers for the given subject
     */
    private Collection<? extends NodeId> getSubscriberNodes(MessageSubject subject) {
        Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.get(subject);
        if (nodeSubscriptions == null) {
            return null;
        }
        return nodeSubscriptions.values()
            .stream()
            .filter(s -> {
                Node node = clusterService.getNode(s.nodeId());
                return node != null && node.state() == Node.State.ACTIVE && !s.isTombstone();
            })
            .map(Subscription::nodeId)
            .collect(Collectors.toList());
    }

    /**
     * Sends a gossip message to an active peer.
     */
    private void gossip() {
        List<NodeId> nodes = clusterService.getNodes()
            .stream()
            .filter(node -> !localNodeId.equals(node.id()))
            .filter(node -> node.state() == Node.State.ACTIVE)
            .map(Node::id)
            .collect(Collectors.toList());

        if (!nodes.isEmpty()) {
            Collections.shuffle(nodes);
            NodeId node = nodes.get(0);
            updateNode(node);
        }
    }

    /**
     * Sends an update to the given node.
     * @param nodeId the node to which to send the update
     */
    private CompletableFuture<Void> updateNode(NodeId nodeId) {
        long updateTime = System.currentTimeMillis();
        long lastUpdateTime = updateTimes.getOrDefault(nodeId.id(), 0L);

        Collection<Subscription> subscriptions = new ArrayList<>();
        subjectSubscriptions.values().forEach(ns -> ns.values()
            .stream()
            .filter(subscription -> subscription.timestamp().unixTimestamp() >= lastUpdateTime)
            .forEach(subscriptions::add));

        CompletableFuture<Void> future = new CompletableFuture<>();
        clusterCommunicator.sendAndReceive(GOSSIP_MESSAGE_SUBJECT, subscriptions, SERIALIZER::encode, SERIALIZER::decode, nodeId)
            .whenComplete((result, error) -> {
                if (error == null) {
                    updateTimes.put(nodeId, updateTime);
                }
                future.complete(null);
            });
        return future;
    }

    /**
     * Purges tombstones from the subscription list.
     */
    private void purgeTombstones() {
        long minTombstoneTime = clusterService.getNodes()
            .stream()
            .map(node -> updateTimes.getOrDefault(node.id(), 0L))
            .reduce(Math::min)
            .orElse(0L);
        for (Map<NodeId, Subscription> nodeSubscriptions : subjectSubscriptions.values()) {
            Iterator<Map.Entry<NodeId, Subscription>> nodeSubscriptionIterator =
                nodeSubscriptions.entrySet().iterator();
            while (nodeSubscriptionIterator.hasNext()) {
                Subscription subscription = nodeSubscriptionIterator.next().getValue();
                if (subscription.isTombstone() && subscription.timestamp().unixTimestamp() < minTombstoneTime) {
                    nodeSubscriptionIterator.remove();
                }
            }
        }
    }

    @Override
    public CompletableFuture<ClusterEventService> open() {
        gossipExecutor = Executors.newSingleThreadScheduledExecutor(
            Threads.namedThreads("atomix-cluster-event-executor-%d", LOGGER));
        gossipExecutor.scheduleAtFixedRate(
            this::gossip,
            GOSSIP_INTERVAL_MILLIS,
            GOSSIP_INTERVAL_MILLIS,
            TimeUnit.MILLISECONDS);
        gossipExecutor.scheduleAtFixedRate(
            this::purgeTombstones,
            TOMBSTONE_EXPIRATION_MILLIS,
            TOMBSTONE_EXPIRATION_MILLIS,
            TimeUnit.MILLISECONDS);
        clusterCommunicator.<Collection<Subscription>, Void>addSubscriber(GOSSIP_MESSAGE_SUBJECT, SERIALIZER::decode, subscriptions -> {
            update(subscriptions);
            return null;
        }, SERIALIZER::encode, gossipExecutor);
        LOGGER.info("Started");
        return CompletableFuture.completedFuture(this);
    }

    /**
     * Handles a collection of subscription updates received via the gossip protocol.
     * @param subscriptions a collection of subscriptions provided by the sender
     */
    private void update(Collection<Subscription> subscriptions) {
        for (Subscription subscription : subscriptions) {
            Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.computeIfAbsent(
                subscription.subject(), s -> Maps.newConcurrentMap());
            Subscription existingSubscription = nodeSubscriptions.get(subscription.nodeId());
            if (existingSubscription == null
                || existingSubscription.logicalTimestamp().isOlderThan(subscription.logicalTimestamp())) {
                nodeSubscriptions.put(subscription.nodeId(), subscription);
                setSubscriberIterator(subscription.subject());
            }
        }
    }

    /**
     * Resets the iterator for the given message subject.
     * @param subject the subject for which to reset the iterator
     */
    private synchronized void setSubscriberIterator(MessageSubject subject) {
        Collection<? extends NodeId> subscriberNodes = getSubscriberNodes(subject);
        if (subscriberNodes != null && !subscriberNodes.isEmpty()) {
            subjectIterators.put(subject, new SubscriberIterator(subscriberNodes));
        } else {
            subjectIterators.remove(subject);
        }
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public CompletableFuture<Void> close() {
        if (gossipExecutor != null) {
            gossipExecutor.shutdown();
        }
        LOGGER.info("Stopped");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isClosed() {
        return !open.get();
    }

    /**
     * Subscriber iterator that iterates subscribers in a loop.
     */
    private static class SubscriberIterator implements Iterator<NodeId> {
        private final AtomicInteger counter = new AtomicInteger();
        private final NodeId[] subscribers;
        private final int length;

        SubscriberIterator(Collection<? extends NodeId> subscribers) {
            this.length = subscribers.size();
            this.subscribers = subscribers.toArray(new NodeId[length]);
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public NodeId next() {
            return subscribers[counter.incrementAndGet() % length];
        }
    }
}