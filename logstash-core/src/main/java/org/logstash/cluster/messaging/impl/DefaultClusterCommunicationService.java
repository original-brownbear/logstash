package org.logstash.cluster.messaging.impl;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.logstash.cluster.cluster.ClusterService;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.messaging.ClusterCommunicationService;
import org.logstash.cluster.messaging.Endpoint;
import org.logstash.cluster.messaging.ManagedClusterCommunicationService;
import org.logstash.cluster.messaging.MessageSubject;
import org.logstash.cluster.messaging.MessagingService;
import org.logstash.cluster.utils.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster communication service implementation.
 */
public class DefaultClusterCommunicationService implements ManagedClusterCommunicationService {

    protected final ClusterService cluster;
    protected final MessagingService messagingService;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeId localNodeId;
    private final AtomicBoolean open = new AtomicBoolean();

    public DefaultClusterCommunicationService(final ClusterService cluster, final MessagingService messagingService) {
        this.cluster = Preconditions.checkNotNull(cluster, "clusterService cannot be null");
        this.messagingService = Preconditions.checkNotNull(messagingService, "messagingService cannot be null");
        this.localNodeId = cluster.getLocalNode().id();
    }

    @Override
    public CompletableFuture<ClusterCommunicationService> open() {
        if (open.compareAndSet(false, true)) {
            log.info("Started");
        }
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public CompletableFuture<Void> close() {
        if (open.compareAndSet(true, false)) {
            log.info("Stopped");
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isClosed() {
        return !isOpen();
    }

    @Override
    public <M> void broadcast(final MessageSubject subject, final M message,
        final Function<M, byte[]> encoder) {
        multicast(subject, message, encoder, cluster.getNodes()
            .stream()
            .filter(node -> !Objects.equal(node, cluster.getLocalNode()))
            .map(Node::id)
            .collect(Collectors.toSet()));
    }

    @Override
    public <M> void broadcastIncludeSelf(final MessageSubject subject, final M message,
        final Function<M, byte[]> encoder) {
        multicast(subject, message, encoder, cluster.getNodes()
            .stream()
            .map(Node::id)
            .collect(Collectors.toSet()));
    }

    @Override
    public <M> CompletableFuture<Void> unicast(
        final MessageSubject subject,
        final M message,
        final Function<M, byte[]> encoder,
        final NodeId toNodeId) {
        try {
            final byte[] payload = new ClusterMessage(
                localNodeId,
                subject,
                encoder.apply(message)
            ).getBytes();
            return doUnicast(subject, payload, toNodeId);
        } catch (final Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public <M> void multicast(
        final MessageSubject subject,
        final M message,
        final Function<M, byte[]> encoder,
        final Set<NodeId> nodes) {
        final byte[] payload = new ClusterMessage(
            localNodeId,
            subject,
            encoder.apply(message))
            .getBytes();
        nodes.forEach(nodeId -> doUnicast(subject, payload, nodeId));
    }

    @Override
    public <M, R> CompletableFuture<R> sendAndReceive(
        final MessageSubject subject,
        final M message,
        final Function<M, byte[]> encoder,
        final Function<byte[], R> decoder,
        final NodeId toNodeId) {
        try {
            final ClusterMessage envelope = new ClusterMessage(
                cluster.getLocalNode().id(),
                subject,
                encoder.apply(message));
            return sendAndReceive(subject, envelope.getBytes(), toNodeId).thenApply(decoder);
        } catch (final Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    private CompletableFuture<byte[]> sendAndReceive(final MessageSubject subject, final byte[] payload, final NodeId toNodeId) {
        final Node node = cluster.getNode(toNodeId);
        Preconditions.checkArgument(node != null, "Unknown nodeId: %s", toNodeId);
        return messagingService.sendAndReceive(node.endpoint(), subject.toString(), payload);
    }

    @Override
    public <M, R> CompletableFuture<Void> addSubscriber(final MessageSubject subject,
        final Function<byte[], M> decoder,
        final Function<M, R> handler,
        final Function<R, byte[]> encoder,
        final Executor executor) {
        messagingService.registerHandler(subject.toString(),
            new InternalMessageResponder<>(decoder, encoder, m -> {
                final CompletableFuture<R> responseFuture = new CompletableFuture<>();
                executor.execute(() -> {
                    try {
                        responseFuture.complete(handler.apply(m));
                    } catch (final Exception e) {
                        responseFuture.completeExceptionally(e);
                    }
                });
                return responseFuture;
            }));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public <M, R> CompletableFuture<Void> addSubscriber(final MessageSubject subject,
        final Function<byte[], M> decoder,
        final Function<M, CompletableFuture<R>> handler,
        final Function<R, byte[]> encoder) {
        messagingService.registerHandler(subject.toString(),
            new InternalMessageResponder<>(decoder, encoder, handler));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public <M> CompletableFuture<Void> addSubscriber(final MessageSubject subject,
        final Function<byte[], M> decoder,
        final Consumer<M> handler,
        final Executor executor) {
        messagingService.registerHandler(subject.toString(),
            new InternalMessageConsumer<>(decoder, handler),
            executor);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void removeSubscriber(final MessageSubject subject) {
        messagingService.unregisterHandler(subject.toString());
    }

    private CompletableFuture<Void> doUnicast(final MessageSubject subject, final byte[] payload, final NodeId toNodeId) {
        final Node node = cluster.getNode(toNodeId);
        Preconditions.checkArgument(node != null, "Unknown nodeId: %s", toNodeId);
        return messagingService.sendAsync(node.endpoint(), subject.toString(), payload);
    }

    private static class InternalMessageResponder<M, R> implements BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> {
        private final Function<byte[], M> decoder;
        private final Function<R, byte[]> encoder;
        private final Function<M, CompletableFuture<R>> handler;

        public InternalMessageResponder(final Function<byte[], M> decoder,
            final Function<R, byte[]> encoder,
            final Function<M, CompletableFuture<R>> handler) {
            this.decoder = decoder;
            this.encoder = encoder;
            this.handler = handler;
        }

        @Override
        public CompletableFuture<byte[]> apply(final Endpoint sender, final byte[] bytes) {
            return handler.apply(decoder.apply(ClusterMessage.fromBytes(bytes).payload())).thenApply(encoder);
        }
    }

    private static class InternalMessageConsumer<M> implements BiConsumer<Endpoint, byte[]> {
        private final Function<byte[], M> decoder;
        private final Consumer<M> consumer;

        public InternalMessageConsumer(final Function<byte[], M> decoder, final Consumer<M> consumer) {
            this.decoder = decoder;
            this.consumer = consumer;
        }

        @Override
        public void accept(final Endpoint sender, final byte[] bytes) {
            consumer.accept(decoder.apply(ClusterMessage.fromBytes(bytes).payload()));
        }
    }
}
