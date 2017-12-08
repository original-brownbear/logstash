package org.logstash.cluster.protocols.raft.proxy.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.protocols.raft.event.RaftEvent;
import org.logstash.cluster.protocols.raft.operation.RaftOperation;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.proxy.RaftProxyClient;
import org.logstash.cluster.protocols.raft.service.ServiceType;
import org.logstash.cluster.protocols.raft.session.SessionId;
import org.logstash.cluster.utils.concurrent.Futures;
import org.logstash.cluster.utils.concurrent.OrderedFuture;
import org.logstash.cluster.utils.concurrent.Scheduled;
import org.logstash.cluster.utils.concurrent.Scheduler;

/**
 * Raft proxy that supports recovery.
 */
public class RecoveringRaftProxyClient implements RaftProxyClient {

    private static final Logger LOGGER = LogManager.getLogger(RecoveringRaftProxyClient.class);

    private static final SessionId DEFAULT_SESSION_ID = SessionId.from(0);
    private final String name;
    private final ServiceType serviceType;
    private final RaftProxyClient.Builder proxyClientBuilder;
    private final Scheduler scheduler;
    private final Set<Consumer<RaftProxy.State>> stateChangeListeners = Sets.newCopyOnWriteArraySet();
    private final Set<Consumer<RaftEvent>> eventListeners = Sets.newCopyOnWriteArraySet();
    private volatile OrderedFuture<RaftProxyClient> clientFuture;
    private volatile RaftProxyClient client;
    private volatile RaftProxy.State state = RaftProxy.State.SUSPENDED;
    private Scheduled recoverTask;
    private volatile boolean open = false;

    public RecoveringRaftProxyClient(String clientId, String name, ServiceType serviceType, RaftProxyClient.Builder proxyClientBuilder, Scheduler scheduler) {
        this.name = Preconditions.checkNotNull(name);
        this.serviceType = Preconditions.checkNotNull(serviceType);
        this.proxyClientBuilder = Preconditions.checkNotNull(proxyClientBuilder);
        this.scheduler = Preconditions.checkNotNull(scheduler);
    }

    @Override
    public SessionId sessionId() {
        RaftProxyClient client = this.client;
        return client != null ? client.sessionId() : DEFAULT_SESSION_ID;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ServiceType serviceType() {
        return serviceType;
    }

    @Override
    public RaftProxy.State getState() {
        return state;
    }

    @Override
    public void addStateChangeListener(Consumer<RaftProxy.State> listener) {
        stateChangeListeners.add(listener);
    }

    @Override
    public void removeStateChangeListener(Consumer<RaftProxy.State> listener) {
        stateChangeListeners.remove(listener);
    }

    @Override
    public CompletableFuture<byte[]> execute(RaftOperation operation) {
        checkOpen();
        RaftProxyClient client = this.client;
        if (client != null) {
            return client.execute(operation);
        } else {
            return clientFuture.thenCompose(c -> c.execute(operation));
        }
    }

    /**
     * Verifies that the client is open.
     */
    private void checkOpen() {
        Preconditions.checkState(isOpen(), "client not open");
    }

    @Override
    public synchronized void addEventListener(Consumer<RaftEvent> consumer) {
        checkOpen();
        eventListeners.add(consumer);
        RaftProxyClient client = this.client;
        if (client != null) {
            client.addEventListener(consumer);
        }
    }

    @Override
    public synchronized void removeEventListener(Consumer<RaftEvent> consumer) {
        checkOpen();
        eventListeners.remove(consumer);
        RaftProxyClient client = this.client;
        if (client != null) {
            client.removeEventListener(consumer);
        }
    }

    /**
     * Sets the session state.
     * @param state the session state
     */
    private synchronized void onStateChange(RaftProxy.State state) {
        if (this.state != state) {
            if (state == RaftProxy.State.CLOSED) {
                if (open) {
                    onStateChange(RaftProxy.State.SUSPENDED);
                    recover();
                } else {
                    LOGGER.debug("State changed: {}", state);
                    this.state = state;
                    stateChangeListeners.forEach(l -> l.accept(state));
                }
            } else {
                LOGGER.debug("State changed: {}", state);
                this.state = state;
                stateChangeListeners.forEach(l -> l.accept(state));
            }
        }
    }

    /**
     * Recovers the client.
     */
    private void recover() {
        client = null;
        openClient();
    }

    /**
     * Opens a new client.
     * @return a future to be completed once the client has been opened
     */
    private CompletableFuture<RaftProxyClient> openClient() {
        if (open) {
            LOGGER.debug("Opening proxy session");

            clientFuture = new OrderedFuture<>();
            openClient(clientFuture);

            return clientFuture.thenApply(client -> {
                synchronized (this) {
                    this.client = client;
                    client.addStateChangeListener(this::onStateChange);
                    eventListeners.forEach(client::addEventListener);
                    onStateChange(RaftProxy.State.CONNECTED);
                }
                return client;
            });
        }
        return Futures.exceptionalFuture(new IllegalStateException("Client not open"));
    }

    /**
     * Opens a new client, completing the provided future only once the client has been opened.
     * @param future the future to be completed once the client is opened
     */
    private void openClient(CompletableFuture<RaftProxyClient> future) {
        proxyClientBuilder.buildAsync().whenComplete((client, error) -> {
            if (error == null) {
                future.complete(client);
            } else {
                recoverTask = scheduler.schedule(Duration.ofSeconds(1), () -> openClient(future));
            }
        });
    }

    @Override
    public synchronized CompletableFuture<RaftProxyClient> open() {
        if (!open) {
            open = true;
            return openClient().thenApply(c -> this);
        }
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized CompletableFuture<Void> close() {
        if (open) {
            open = false;
            if (recoverTask != null) {
                recoverTask.cancel();
            }

            RaftProxyClient client = this.client;
            if (client != null) {
                return client.close();
            } else {
                return clientFuture.thenCompose(c -> c.close());
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isClosed() {
        return !open;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", client.name())
            .add("serviceType", client.serviceType())
            .add("state", state)
            .toString();
    }
}
