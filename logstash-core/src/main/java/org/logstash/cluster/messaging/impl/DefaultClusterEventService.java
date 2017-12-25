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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

/**
 * Cluster event service.
 */
public class DefaultClusterEventService implements ManagedClusterEventService {
    private static final Logger LOGGER = LogManager.getLogger(DefaultClusterEventService.class);

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(NodeId.class)
        .register(Subscription.class)
        .register(MessageSubject.class)
        .register(LogicalTimestamp.class)
        .register(WallClockTimestamp.class)
        .build());

    private static final MessageSubject GOSSIP_MESSAGE_SUBJECT = new MessageSubject("ClusterEventService-update");

    private static final long GOSSIP_INTERVAL_MILLIS = 1000L;
    private static final long TOMBSTONE_EXPIRATION_MILLIS = (long) (1000 * 60);

    private final ClusterService clusterService;
    private final ClusterCommunicationService clusterCommunicator;
    private final NodeId localNodeId;
    private final AtomicLong logicalTime = new AtomicLong();
    private final Map<NodeId, Long> updateTimes = Maps.newConcurrentMap();
    private final Map<MessageSubject, Map<NodeId, Subscription>> subjectSubscriptions = Maps.newConcurrentMap();
    private final Map<MessageSubject, DefaultClusterEventService.SubscriberIterator> subjectIterators = Maps.newConcurrentMap();
    private final AtomicBoolean open = new AtomicBoolean();
    private ScheduledExecutorService gossipExecutor;

    public DefaultClusterEventService(final ClusterService clusterService, final ClusterCommunicationService clusterCommunicator) {
        this.clusterService = clusterService;
        this.clusterCommunicator = clusterCommunicator;
        this.localNodeId = clusterService.getLocalNode().id();
    }

    @Override
    public <M> void broadcast(final MessageSubject subject, final M message, final Function<M, byte[]> encoder) {
        final Collection<? extends NodeId> subscribers = getSubscriberNodes(subject);
        if (subscribers != null) {
            subscribers.forEach(nodeId -> clusterCommunicator.unicast(subject, message, encoder, nodeId));
        }
    }

    @Override
    public <M> CompletableFuture<Void> unicast(final MessageSubject subject, final M message, final Function<M, byte[]> encoder) {
        final NodeId nodeId = getNextNodeId(subject);
        if (nodeId != null) {
            return clusterCommunicator.unicast(subject, message, encoder, nodeId);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public <M, R> CompletableFuture<R> sendAndReceive(final MessageSubject subject, final M message, final Function<M, byte[]> encoder, final Function<byte[], R> decoder) {
        final NodeId nodeId = getNextNodeId(subject);
        if (nodeId == null) {
            return Futures.exceptionalFuture(new MessagingException.NoRemoteHandler());
        }
        return clusterCommunicator.sendAndReceive(subject, message, encoder, decoder, nodeId);
    }

    @Override
    public <M, R> CompletableFuture<Void> addSubscriber(final MessageSubject subject, final Function<byte[], M> decoder, final Function<M, R> handler, final Function<R, byte[]> encoder, final Executor executor) {
        return clusterCommunicator.addSubscriber(subject, decoder, handler, encoder, executor)
            .thenCompose(v -> registerSubscriber(subject));
    }

    /**
     * Registers the node as a subscriber for the given subject.
     * @param subject the subject for which to register the node as a subscriber
     */
    private synchronized CompletableFuture<Void> registerSubscriber(final MessageSubject subject) {
        final Map<NodeId, Subscription> nodeSubscriptions =
            subjectSubscriptions.computeIfAbsent(subject, s -> Maps.newConcurrentMap());
        final Subscription subscription = new Subscription(
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
        final List<CompletableFuture<Void>> futures = clusterService.getNodes()
            .stream()
            .filter(node -> !localNodeId.equals(node.id()))
            .map(Node::id)
            .map(this::updateNode)
            .collect(Collectors.toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }

    @Override
    public <M, R> CompletableFuture<Void> addSubscriber(final MessageSubject subject, final Function<byte[], M> decoder, final Function<M, CompletableFuture<R>> handler, final Function<R, byte[]> encoder) {
        registerSubscriber(subject);
        return clusterCommunicator.addSubscriber(subject, decoder, handler, encoder)
            .thenCompose(v -> registerSubscriber(subject));
    }

    @Override
    public <M> CompletableFuture<Void> addSubscriber(final MessageSubject subject, final Function<byte[], M> decoder, final Consumer<M> handler, final Executor executor) {
        return clusterCommunicator.addSubscriber(subject, decoder, handler, executor)
            .thenCompose(v -> registerSubscriber(subject));
    }

    @Override
    public void removeSubscriber(final MessageSubject subject) {
        unregisterSubscriber(subject);
        clusterCommunicator.removeSubscriber(subject);
    }

    /**
     * Unregisters the node as a subscriber for the given subject.
     * @param subject the subject for which to unregister the node as a subscriber
     */
    private synchronized void unregisterSubscriber(final MessageSubject subject) {
        final Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.get(subject);
        if (nodeSubscriptions != null) {
            final Subscription subscription = nodeSubscriptions.get(localNodeId);
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
    private NodeId getNextNodeId(final MessageSubject subject) {
        final DefaultClusterEventService.SubscriberIterator iterator = subjectIterators.get(subject);
        return iterator != null && iterator.hasNext() ? iterator.next() : null;
    }

    /**
     * Returns the set of nodes with subscribers for the given message subject.
     * @param subject the subject for which to return a set of nodes
     * @return a set of nodes with subscribers for the given subject
     */
    private Collection<? extends NodeId> getSubscriberNodes(final MessageSubject subject) {
        final Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.get(subject);
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
        final List<NodeId> nodes = clusterService.getNodes()
            .stream()
            .filter(node -> !localNodeId.equals(node.id()))
            .filter(node -> node.state() == Node.State.ACTIVE)
            .map(Node::id)
            .collect(Collectors.toList());

        if (!nodes.isEmpty()) {
            Collections.shuffle(nodes);
            final NodeId node = nodes.get(0);
            updateNode(node);
        }
    }

    /**
     * Sends an update to the given node.
     * @param nodeId the node to which to send the update
     */
    private CompletableFuture<Void> updateNode(final NodeId nodeId) {
        final long updateTime = System.currentTimeMillis();
        final long lastUpdateTime = updateTimes.getOrDefault(nodeId.id(), 0L);

        final Collection<Subscription> subscriptions = new ArrayList<>();
        subjectSubscriptions.values().forEach(ns -> ns.values()
            .stream()
            .filter(subscription -> subscription.timestamp().unixTimestamp() >= lastUpdateTime)
            .forEach(subscriptions::add));

        final CompletableFuture<Void> future = new CompletableFuture<>();
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
        final long minTombstoneTime = clusterService.getNodes()
            .stream()
            .map(node -> updateTimes.getOrDefault(node.id(), 0L))
            .reduce(Math::min)
            .orElse(0L);
        for (final Map<NodeId, Subscription> nodeSubscriptions : subjectSubscriptions.values()) {
            final Iterator<Map.Entry<NodeId, Subscription>> nodeSubscriptionIterator =
                nodeSubscriptions.entrySet().iterator();
            while (nodeSubscriptionIterator.hasNext()) {
                final Subscription subscription = nodeSubscriptionIterator.next().getValue();
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
    private void update(final Collection<Subscription> subscriptions) {
        for (final Subscription subscription : subscriptions) {
            final Map<NodeId, Subscription> nodeSubscriptions = subjectSubscriptions.computeIfAbsent(
                subscription.subject(), s -> Maps.newConcurrentMap());
            final Subscription existingSubscription = nodeSubscriptions.get(subscription.nodeId());
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
    private synchronized void setSubscriberIterator(final MessageSubject subject) {
        final Collection<? extends NodeId> subscriberNodes = getSubscriberNodes(subject);
        if (subscriberNodes != null && !subscriberNodes.isEmpty()) {
            subjectIterators.put(subject, new DefaultClusterEventService.SubscriberIterator(subscriberNodes));
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

        SubscriberIterator(final Collection<? extends NodeId> subscribers) {
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
