package org.logstash.cluster.protocols.raft.proxy.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.protocols.raft.event.RaftEvent;
import org.logstash.cluster.protocols.raft.protocol.PublishRequest;
import org.logstash.cluster.protocols.raft.protocol.RaftClientProtocol;
import org.logstash.cluster.protocols.raft.protocol.ResetRequest;

/**
 * Client session message listener.
 */
final class RaftProxyListener {

    private static final Logger LOGGER = LogManager.getLogger(RaftProxyListener.class);

    private final RaftClientProtocol protocol;
    private final MemberSelector memberSelector;
    private final RaftProxyState state;
    private final Set<Consumer<RaftEvent>> listeners = Sets.newLinkedHashSet();
    private final RaftProxySequencer sequencer;
    private final Executor executor;

    public RaftProxyListener(final RaftClientProtocol protocol, final MemberSelector memberSelector,
        final RaftProxyState state, final RaftProxySequencer sequencer, final Executor executor) {
        this.protocol = Preconditions.checkNotNull(protocol, "protocol cannot be null");
        this.memberSelector = Preconditions.checkNotNull(memberSelector, "nodeSelector cannot be null");
        this.state = Preconditions.checkNotNull(state, "state cannot be null");
        this.sequencer = Preconditions.checkNotNull(sequencer, "sequencer cannot be null");
        this.executor = Preconditions.checkNotNull(executor, "executor cannot be null");
        protocol.registerPublishListener(state.getSessionId(), this::handlePublish, executor);
    }

    /**
     * Adds an event listener to the session.
     * @param listener the event listener callback
     */
    public void addEventListener(final Consumer<RaftEvent> listener) {
        executor.execute(() -> listeners.add(listener));
    }

    /**
     * Removes an event listener from the session.
     * @param listener the event listener callback
     */
    public void removeEventListener(final Consumer<RaftEvent> listener) {
        executor.execute(() -> listeners.remove(listener));
    }

    /**
     * Handles a publish request.
     * @param request The publish request to handle.
     */
    @SuppressWarnings("unchecked")
    private void handlePublish(final PublishRequest request) {
        LOGGER.trace("Received {}", request);

        // If the request is for another session ID, this may be a session that was previously opened
        // for this client.
        if (request.session() != state.getSessionId().id()) {
            LOGGER.trace("Inconsistent session ID: {}", request.session());
            return;
        }

        // Store eventIndex in a local variable to prevent multiple volatile reads.
        final long eventIndex = state.getEventIndex();

        // If the request event index has already been processed, return.
        if (request.eventIndex() <= eventIndex) {
            LOGGER.trace("Duplicate event index {}", request.eventIndex());
            return;
        }

        // If the request's previous event index doesn't equal the previous received event index,
        // respond with an undefined error and the last index received. This will cause the cluster
        // to resend events starting at eventIndex + 1.
        if (request.previousIndex() != eventIndex) {
            LOGGER.trace("Inconsistent event index: {}", request.previousIndex());
            final ResetRequest resetRequest = ResetRequest.builder()
                .withSession(state.getSessionId().id())
                .withIndex(eventIndex)
                .build();
            LOGGER.trace("Sending {}", resetRequest);
            protocol.reset(memberSelector.members(), resetRequest);
            return;
        }

        // Store the event index. This will be used to verify that events are received in sequential order.
        state.setEventIndex(request.eventIndex());

        sequencer.sequenceEvent(request, () -> {
            for (final RaftEvent event : request.events()) {
                for (final Consumer<RaftEvent> listener : listeners) {
                    listener.accept(event);
                }
            }
        });
    }

    /**
     * Closes the session event listener.
     * @return A completable future to be completed once the listener is closed.
     */
    public CompletableFuture<Void> close() {
        protocol.unregisterPublishListener(state.getSessionId());
        return CompletableFuture.completedFuture(null);
    }
}
