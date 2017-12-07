package org.logstash.cluster.messaging;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import org.logstash.cluster.serializer.kryo.serializers.DefaultSerializers;

/**
 * Cluster event service.
 */
public interface ClusterEventService {

    /**
     * Broadcasts a message to all controller nodes.
     * @param subject message subject
     * @param message message to send
     * @param <M> message type
     */
    default <M> void broadcast(
        MessageSubject subject,
        M message) {
        broadcast(subject, message, DefaultSerializers.BASIC::encode);
    }

    /**
     * Broadcasts a message to all controller nodes.
     * @param subject message subject
     * @param message message to send
     * @param encoder function for encoding message to byte[]
     * @param <M> message type
     */
    <M> void broadcast(
        MessageSubject subject,
        M message,
        Function<M, byte[]> encoder);

    /**
     * Sends a message to the specified controller node.
     * @param subject message subject
     * @param message message to send
     * @param <M> message type
     * @return future that is completed when the message is sent
     */
    default <M> CompletableFuture<Void> unicast(
        MessageSubject subject,
        M message) {
        return unicast(subject, message, DefaultSerializers.BASIC::encode);
    }

    /**
     * Sends a message to the specified controller node.
     * @param message message to send
     * @param subject message subject
     * @param encoder function for encoding message to byte[]
     * @param <M> message type
     * @return future that is completed when the message is sent
     */
    <M> CompletableFuture<Void> unicast(
        MessageSubject subject,
        M message,
        Function<M, byte[]> encoder);

    /**
     * Sends a message and expects a reply.
     * @param subject message subject
     * @param message message to send
     * @param <M> request type
     * @param <R> reply type
     * @return reply future
     */
    default <M, R> CompletableFuture<R> sendAndReceive(
        MessageSubject subject,
        M message) {
        return sendAndReceive(subject, message, DefaultSerializers.BASIC::encode, DefaultSerializers.BASIC::decode);
    }

    /**
     * Sends a message and expects a reply.
     * @param subject message subject
     * @param message message to send
     * @param encoder function for encoding request to byte[]
     * @param decoder function for decoding response from byte[]
     * @param <M> request type
     * @param <R> reply type
     * @return reply future
     */
    <M, R> CompletableFuture<R> sendAndReceive(
        MessageSubject subject,
        M message,
        Function<M, byte[]> encoder,
        Function<byte[], R> decoder);

    /**
     * Adds a new subscriber for the specified message subject.
     * @param subject message subject
     * @param handler handler function that processes the incoming message and produces a reply
     * @param executor executor to run this handler on
     * @param <M> incoming message type
     * @param <R> reply message type
     * @return future to be completed once the subscription has been propagated
     */
    default <M, R> CompletableFuture<Void> addSubscriber(
        MessageSubject subject,
        Function<M, R> handler,
        Executor executor) {
        return addSubscriber(subject, DefaultSerializers.BASIC::decode, handler, DefaultSerializers.BASIC::encode, executor);
    }

    /**
     * Adds a new subscriber for the specified message subject.
     * @param subject message subject
     * @param decoder decoder for resurrecting incoming message
     * @param handler handler function that processes the incoming message and produces a reply
     * @param encoder encoder for serializing reply
     * @param executor executor to run this handler on
     * @param <M> incoming message type
     * @param <R> reply message type
     * @return future to be completed once the subscription has been propagated
     */
    <M, R> CompletableFuture<Void> addSubscriber(
        MessageSubject subject,
        Function<byte[], M> decoder,
        Function<M, R> handler,
        Function<R, byte[]> encoder,
        Executor executor);

    /**
     * Adds a new subscriber for the specified message subject.
     * @param subject message subject
     * @param handler handler function that processes the incoming message and produces a reply
     * @param <M> incoming message type
     * @param <R> reply message type
     * @return future to be completed once the subscription has been propagated
     */
    default <M, R> CompletableFuture<Void> addSubscriber(
        MessageSubject subject,
        Function<M, CompletableFuture<R>> handler) {
        return addSubscriber(subject, DefaultSerializers.BASIC::decode, handler, DefaultSerializers.BASIC::encode);
    }

    /**
     * Adds a new subscriber for the specified message subject.
     * @param subject message subject
     * @param decoder decoder for resurrecting incoming message
     * @param handler handler function that processes the incoming message and produces a reply
     * @param encoder encoder for serializing reply
     * @param <M> incoming message type
     * @param <R> reply message type
     * @return future to be completed once the subscription has been propagated
     */
    <M, R> CompletableFuture<Void> addSubscriber(
        MessageSubject subject,
        Function<byte[], M> decoder,
        Function<M, CompletableFuture<R>> handler,
        Function<R, byte[]> encoder);

    /**
     * Adds a new subscriber for the specified message subject.
     * @param subject message subject
     * @param handler handler for handling message
     * @param executor executor to run this handler on
     * @param <M> incoming message type
     * @return future to be completed once the subscription has been propagated
     */
    default <M> CompletableFuture<Void> addSubscriber(
        MessageSubject subject,
        Consumer<M> handler,
        Executor executor) {
        return addSubscriber(subject, DefaultSerializers.BASIC::decode, handler, executor);
    }

    /**
     * Adds a new subscriber for the specified message subject.
     * @param subject message subject
     * @param decoder decoder to resurrecting incoming message
     * @param handler handler for handling message
     * @param executor executor to run this handler on
     * @param <M> incoming message type
     * @return future to be completed once the subscription has been propagated
     */
    <M> CompletableFuture<Void> addSubscriber(
        MessageSubject subject,
        Function<byte[], M> decoder,
        Consumer<M> handler,
        Executor executor);

    /**
     * Removes a subscriber for the specified message subject.
     * @param subject message subject
     */
    void removeSubscriber(MessageSubject subject);

}
