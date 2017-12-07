/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.logstash.cluster.protocols.raft.proxy.impl;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.logstash.cluster.protocols.raft.RaftError;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.protocol.CloseSessionRequest;
import org.logstash.cluster.protocols.raft.protocol.CloseSessionResponse;
import org.logstash.cluster.protocols.raft.protocol.CommandRequest;
import org.logstash.cluster.protocols.raft.protocol.CommandResponse;
import org.logstash.cluster.protocols.raft.protocol.KeepAliveRequest;
import org.logstash.cluster.protocols.raft.protocol.KeepAliveResponse;
import org.logstash.cluster.protocols.raft.protocol.MetadataRequest;
import org.logstash.cluster.protocols.raft.protocol.MetadataResponse;
import org.logstash.cluster.protocols.raft.protocol.OpenSessionRequest;
import org.logstash.cluster.protocols.raft.protocol.OpenSessionResponse;
import org.logstash.cluster.protocols.raft.protocol.QueryRequest;
import org.logstash.cluster.protocols.raft.protocol.QueryResponse;
import org.logstash.cluster.protocols.raft.protocol.RaftClientProtocol;
import org.logstash.cluster.protocols.raft.protocol.RaftRequest;
import org.logstash.cluster.protocols.raft.protocol.RaftResponse;
import org.logstash.cluster.utils.concurrent.ThreadContext;
import org.logstash.cluster.utils.logging.ContextualLoggerFactory;
import org.logstash.cluster.utils.logging.LoggerContext;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client connection that recursively connects to servers in the cluster and attempts to submit requests.
 */
public class RaftProxyConnection {
    private static final Predicate<RaftResponse> COMPLETE_PREDICATE = response ->
        response.status() == RaftResponse.Status.OK
            || response.error().type() == RaftError.Type.COMMAND_FAILURE
            || response.error().type() == RaftError.Type.QUERY_FAILURE
            || response.error().type() == RaftError.Type.APPLICATION_ERROR
            || response.error().type() == RaftError.Type.UNKNOWN_CLIENT
            || response.error().type() == RaftError.Type.UNKNOWN_SESSION
            || response.error().type() == RaftError.Type.UNKNOWN_SERVICE
            || response.error().type() == RaftError.Type.PROTOCOL_ERROR;

    private final Logger log;
    private final RaftClientProtocol protocol;
    private final MemberSelector selector;
    private final ThreadContext context;
    private MemberId member;

    public RaftProxyConnection(final RaftClientProtocol protocol, final MemberSelector selector, final ThreadContext context, final LoggerContext loggerContext) {
        this.protocol = checkNotNull(protocol, "protocol cannot be null");
        this.selector = checkNotNull(selector, "selector cannot be null");
        this.context = checkNotNull(context, "context cannot be null");
        this.log = ContextualLoggerFactory.getLogger(getClass(), loggerContext);
    }

    /**
     * Resets the member selector.
     */
    public void reset() {
        selector.reset();
    }

    /**
     * Resets the member selector.
     * @param leader the selector leader
     * @param servers the selector servers
     */
    public void reset(final MemberId leader, final Collection<MemberId> servers) {
        selector.reset(leader, servers);
    }

    /**
     * Returns the current selector leader.
     * @return The current selector leader.
     */
    public MemberId leader() {
        return selector.leader();
    }

    /**
     * Returns the current set of members.
     * @return The current set of members.
     */
    public Collection<MemberId> members() {
        return selector.members();
    }

    /**
     * Sends an open session request to the given node.
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<OpenSessionResponse> openSession(final OpenSessionRequest request) {
        final CompletableFuture<OpenSessionResponse> future = new CompletableFuture<>();
        if (context.isCurrentContext()) {
            sendRequest(request, protocol::openSession, next(), future);
        } else {
            context.execute(() -> sendRequest(request, protocol::openSession, next(), future));
        }
        return future;
    }

    /**
     * Sends a close session request to the given node.
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<CloseSessionResponse> closeSession(final CloseSessionRequest request) {
        final CompletableFuture<CloseSessionResponse> future = new CompletableFuture<>();
        if (context.isCurrentContext()) {
            sendRequest(request, protocol::closeSession, next(), future);
        } else {
            context.execute(() -> sendRequest(request, protocol::closeSession, next(), future));
        }
        return future;
    }

    /**
     * Sends a keep alive request to the given node.
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<KeepAliveResponse> keepAlive(final KeepAliveRequest request) {
        final CompletableFuture<KeepAliveResponse> future = new CompletableFuture<>();
        if (context.isCurrentContext()) {
            sendRequest(request, protocol::keepAlive, next(), future);
        } else {
            context.execute(() -> sendRequest(request, protocol::keepAlive, next(), future));
        }
        return future;
    }

    /**
     * Sends a query request to the given node.
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<QueryResponse> query(final QueryRequest request) {
        final CompletableFuture<QueryResponse> future = new CompletableFuture<>();
        if (context.isCurrentContext()) {
            sendRequest(request, protocol::query, next(), future);
        } else {
            context.execute(() -> sendRequest(request, protocol::query, next(), future));
        }
        return future;
    }

    /**
     * Sends a command request to the given node.
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<CommandResponse> command(final CommandRequest request) {
        final CompletableFuture<CommandResponse> future = new CompletableFuture<>();
        if (context.isCurrentContext()) {
            sendRequest(request, protocol::command, next(), future);
        } else {
            context.execute(() -> sendRequest(request, protocol::command, next(), future));
        }
        return future;
    }

    /**
     * Sends a metadata request to the given node.
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<MetadataResponse> metadata(final MetadataRequest request) {
        final CompletableFuture<MetadataResponse> future = new CompletableFuture<>();
        if (context.isCurrentContext()) {
            sendRequest(request, protocol::metadata, next(), future);
        } else {
            context.execute(() -> sendRequest(request, protocol::metadata, next(), future));
        }
        return future;
    }

    /**
     * Sends the given request attempt to the cluster.
     */
    protected <T extends RaftRequest, U extends RaftResponse> void sendRequest(final T request, final BiFunction<MemberId, T, CompletableFuture<U>> sender, final MemberId member, final CompletableFuture<U> future) {
        if (member != null) {
            log.trace("Sending {} to {}", request, member);
            sender.apply(member, request).whenCompleteAsync((r, e) -> {
                if (e != null || r != null) {
                    handleResponse(request, sender, member, r, e, future);
                } else {
                    future.complete(null);
                }
            }, context);
        } else {
            future.completeExceptionally(new ConnectException("Failed to connect to the cluster"));
        }
    }

    /**
     * Resends a request due to a request failure, resetting the connection if necessary.
     */
    @SuppressWarnings("unchecked")
    protected <T extends RaftRequest> void retryRequest(final Throwable cause, final T request, final BiFunction sender, final MemberId member, final CompletableFuture future) {
        // If the connection has not changed, reset it and connect to the next server.
        if (this.member == member) {
            log.trace("Resetting connection. Reason: {}", cause.getMessage());
            this.member = null;
        }

        // Attempt to send the request again.
        sendRequest(request, sender, next(), future);
    }

    /**
     * Handles a response from the cluster.
     */
    @SuppressWarnings("unchecked")
    protected <T extends RaftRequest> void handleResponse(final T request, final BiFunction sender, final MemberId member, final RaftResponse response, Throwable error, final CompletableFuture future) {
        if (error == null) {
            if (COMPLETE_PREDICATE.test(response)) {
                log.trace("Received {} from {}", response, member);
                future.complete(response);
                selector.reset();
            } else {
                retryRequest(response.error().createException(), request, sender, member, future);
            }
        } else {
            if (error instanceof CompletionException) {
                error = error.getCause();
            }
            log.debug("{} failed! Reason: {}", request, error);
            if (error instanceof ConnectException || error instanceof TimeoutException || error instanceof ClosedChannelException) {
                retryRequest(error, request, sender, member, future);
            } else {
                future.completeExceptionally(error);
            }
        }
    }

    /**
     * Connects to the cluster.
     */
    protected MemberId next() {
        // If a connection was already established then use that connection.
        if (member != null) {
            return member;
        }

        if (!selector.hasNext()) {
            log.debug("Failed to connect to the cluster");
            selector.reset();
            return null;
        } else {
            this.member = selector.next();
            return member;
        }
    }
}