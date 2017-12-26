package org.logstash.cluster.messaging.netty;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Key;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.messaging.Endpoint;
import org.logstash.cluster.messaging.ManagedMessagingService;
import org.logstash.cluster.messaging.MessagingException;
import org.logstash.cluster.messaging.MessagingService;
import org.logstash.cluster.utils.concurrent.Threads;

/**
 * Netty based MessagingService.
 */
public class NettyMessagingService implements ManagedMessagingService {
    public static final int DEFAULT_PORT = 5679;
    private static final boolean TLS_ENABLED = true;
    private static final boolean TLS_DISABLED = false;
    private static final String DEFAULT_NAME = "atomix";
    private static final long DEFAULT_TIMEOUT_MILLIS = 500L;
    private static final long HISTORY_EXPIRE_MILLIS = Duration.ofMinutes(1L).toMillis();
    private static final long MIN_TIMEOUT_MILLIS = 250L;
    private static final long MAX_TIMEOUT_MILLIS = 5000L;
    private static final long TIMEOUT_INTERVAL = 50L;
    private static final int WINDOW_SIZE = 100;
    private static final double TIMEOUT_MULTIPLIER = 2.5;
    private static final int CHANNEL_POOL_SIZE = 8;

    private static final byte[] EMPTY_PAYLOAD = new byte[0];
    //TODO CONFIG_DIR is duplicated from ConfigFileBasedClusterMetadataProvider
    private static final String CONFIG_DIR = "../config";
    private static final String KS_FILE_NAME = "atomix.jks";
    private static final File DEFAULT_KS_FILE = new File(CONFIG_DIR, KS_FILE_NAME);
    private static final String DEFAULT_KS_PASSWORD = "changeit";

    private static final Logger LOGGER = LogManager.getLogger(NettyMessagingService.class);

    private final NettyMessagingService.ClientConnection localClientConnection = new LocalClientConnection();
    private final NettyMessagingService.ServerConnection localServerConnection = new NettyMessagingService.LocalServerConnection(null);
    private final Endpoint localEndpoint;
    private final int preamble;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Map<String, BiConsumer<InternalRequest, NettyMessagingService.ServerConnection>> handlers = new ConcurrentHashMap<>();
    private final Map<Channel, NettyMessagingService.RemoteClientConnection> clientConnections = Maps.newConcurrentMap();
    private final Map<Channel, NettyMessagingService.RemoteServerConnection> serverConnections = Maps.newConcurrentMap();
    private final AtomicLong messageIdGenerator = new AtomicLong(0L);
    private final Map<Endpoint, List<CompletableFuture<Channel>>> channels = Maps.newConcurrentMap();
    private boolean enableNettyTls = TLS_ENABLED;
    private TrustManagerFactory trustManager;
    private KeyManagerFactory keyManager;
    private ScheduledFuture<?> timeoutFuture;
    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private Class<? extends ServerChannel> serverChannelClass;
    private Class<? extends Channel> clientChannelClass;
    private ScheduledExecutorService timeoutExecutor;

    NettyMessagingService(final int preamble, final Endpoint endpoint) {
        this.preamble = preamble;
        this.localEndpoint = endpoint;
    }

    /**
     * Returns a new Netty messaging service builder.
     * @return a new Netty messaging service builder
     */
    public static NettyMessagingService.Builder builder() {
        return new NettyMessagingService.Builder();
    }

    @Override
    public CompletableFuture<MessagingService> open() {
        getTlsParameters();
        if (started.get()) {
            LOGGER.warn("Already running at local endpoint: {}", localEndpoint);
            return CompletableFuture.completedFuture(this);
        }

        initEventLoopGroup();
        return startAcceptingConnections().thenRun(() -> {
            timeoutExecutor = Executors.newSingleThreadScheduledExecutor(
                Threads.namedThreads("netty-messaging-timeout-%d", LOGGER));
            timeoutFuture = timeoutExecutor.scheduleAtFixedRate(
                this::timeoutAllCallbacks, TIMEOUT_INTERVAL, TIMEOUT_INTERVAL, TimeUnit.MILLISECONDS);
            started.set(true);
            LOGGER.info("Started");
        }).thenApply(v -> this);
    }

    private void getTlsParameters() {
        // default is TLS enabled unless key stores cannot be loaded
        enableNettyTls = Boolean.parseBoolean(System.getProperty("enableNettyTLS", Boolean.toString(TLS_ENABLED)));

        if (enableNettyTls) {
            enableNettyTls = loadKeyStores();
        }
    }

    private boolean loadKeyStores() {
        // Maintain a local copy of the trust and key managers in case anything goes wrong
        final TrustManagerFactory tmf;
        final KeyManagerFactory kmf;
        try {
            final String ksLocation = System.getProperty("javax.net.ssl.keyStore", DEFAULT_KS_FILE.toString());
            final String tsLocation = System.getProperty("javax.net.ssl.trustStore", DEFAULT_KS_FILE.toString());
            final char[] ksPwd = System.getProperty("javax.net.ssl.keyStorePassword", DEFAULT_KS_PASSWORD).toCharArray();
            final char[] tsPwd = System.getProperty("javax.net.ssl.trustStorePassword", DEFAULT_KS_PASSWORD).toCharArray();

            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            final KeyStore ts = KeyStore.getInstance("JKS");
            ts.load(new FileInputStream(tsLocation), tsPwd);
            tmf.init(ts);

            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            final KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(new FileInputStream(ksLocation), ksPwd);
            kmf.init(ks, ksPwd);
            if (LOGGER.isInfoEnabled()) {
                logKeyStore(ks, ksLocation, ksPwd);
            }
        } catch (final FileNotFoundException e) {
            LOGGER.warn("Disabling TLS for intra-cluster messaging; Could not load cluster key store: {}", e.getMessage());
            return TLS_DISABLED;
        } catch (final Exception e) {
            //TODO we might want to catch exceptions more specifically
            LOGGER.error("Error loading key store; disabling TLS for intra-cluster messaging", e);
            return TLS_DISABLED;
        }
        this.trustManager = tmf;
        this.keyManager = kmf;
        return TLS_ENABLED;
    }

    private void logKeyStore(final KeyStore ks, final String ksLocation, final char[] ksPwd) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Loaded cluster key store from: {}", ksLocation);
            try {
                for (final Enumeration<String> e = ks.aliases(); e.hasMoreElements(); ) {
                    final String alias = e.nextElement();
                    final Key key = ks.getKey(alias, ksPwd);
                    final Certificate[] certs = ks.getCertificateChain(alias);
                    LOGGER.debug("{} -> {}", alias, certs);
                    final byte[] encodedKey;
                    if (certs != null && certs.length > 0) {
                        encodedKey = certs[0].getEncoded();
                    } else {
                        LOGGER.info("Could not find cert chain for {}, using fingerprint of key instead...", alias);
                        encodedKey = key.getEncoded();
                    }
                    // Compute the certificate's fingerprint (use the key if certificate cannot be found)
                    final MessageDigest digest = MessageDigest.getInstance("SHA1");
                    digest.update(encodedKey);
                    final StringJoiner fingerprint = new StringJoiner(":");
                    for (final byte b : digest.digest()) {
                        fingerprint.add(String.format("%02X", b));
                    }
                    LOGGER.info("{} -> {}", alias, fingerprint);
                }
            } catch (final Exception e) {
                LOGGER.warn("Unable to print contents of key store: {}", ksLocation, e);
            }
        }
    }

    @Override
    public boolean isOpen() {
        return started.get();
    }

    @Override
    public CompletableFuture<Void> close() {
        if (started.get()) {
            serverGroup.shutdownGracefully();
            clientGroup.shutdownGracefully();
            timeoutFuture.cancel(false);
            timeoutExecutor.shutdown();
            started.set(false);
        }
        LOGGER.info("Stopped");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isClosed() {
        return !isOpen();
    }

    private void initEventLoopGroup() {
        // try Epoll first and if that does work, use nio.
        try {
            clientGroup = new EpollEventLoopGroup(0, Threads.namedThreads("netty-messaging-event-epoll-client-%d", LOGGER));
            serverGroup = new EpollEventLoopGroup(0, Threads.namedThreads("netty-messaging-event-epoll-server-%d", LOGGER));
            serverChannelClass = EpollServerSocketChannel.class;
            clientChannelClass = EpollSocketChannel.class;
            return;
        } catch (final Throwable e) {
            LOGGER.debug("Failed to initialize native (epoll) transport. "
                + "Reason: {}. Proceeding with nio.", e.getMessage());
        }
        clientGroup = new NioEventLoopGroup(0, Threads.namedThreads("netty-messaging-event-nio-client-%d", LOGGER));
        serverGroup = new NioEventLoopGroup(0, Threads.namedThreads("netty-messaging-event-nio-server-%d", LOGGER));
        serverChannelClass = NioServerSocketChannel.class;
        clientChannelClass = NioSocketChannel.class;
    }

    private CompletableFuture<Void> startAcceptingConnections() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final ServerBootstrap b = new ServerBootstrap().childOption(
            ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(8 * 1024, 32 * 1024)
        )
            .option(ChannelOption.SO_RCVBUF, 1048576)
            .option(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .group(serverGroup, clientGroup)
            .channel(serverChannelClass);
        if (enableNettyTls) {
            b.childHandler(new SslServerCommunicationChannelInitializer());
        } else {
            b.childHandler(new BasicChannelInitializer());
        }
        b.option(ChannelOption.SO_BACKLOG, 128);
        b.childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections.
        b.bind(localEndpoint.port()).addListener(f -> {
            if (f.isSuccess()) {
                LOGGER.info("{} accepting incoming connections on port {}",
                    localEndpoint.host(), localEndpoint.port());
                future.complete(null);
            } else {
                LOGGER.warn("{} failed to bind to port {} due to {}",
                    localEndpoint.host(), localEndpoint.port(), f.cause());
                future.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    /**
     * Times out response callbacks.
     */
    private void timeoutAllCallbacks() {
        // Iterate through all connections and time out callbacks.
        for (final NettyMessagingService.RemoteClientConnection connection : clientConnections.values()) {
            connection.timeoutCallbacks();
        }
    }

    @Override
    public CompletableFuture<Void> sendAsync(final Endpoint ep, final String type, final byte[] payload) {
        final InternalRequest message = new InternalRequest(preamble,
            messageIdGenerator.incrementAndGet(),
            localEndpoint,
            type,
            payload);
        return executeOnPooledConnection(ep, type, c -> c.sendAsync(message), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<byte[]> sendAndReceive(final Endpoint ep, final String type, final byte[] payload) {
        return sendAndReceive(ep, type, payload, MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<byte[]> sendAndReceive(final Endpoint ep, final String type, final byte[] payload, final Executor executor) {
        final long messageId = messageIdGenerator.incrementAndGet();
        final InternalRequest message = new InternalRequest(preamble,
            messageId,
            localEndpoint,
            type,
            payload);
        return executeOnPooledConnection(ep, type, c -> c.sendAndReceive(message), executor);
    }

    @Override
    public void registerHandler(final String type, final BiConsumer<Endpoint, byte[]> handler, final Executor executor) {
        handlers.put(type, (message, connection) -> executor.execute(() ->
            handler.accept(message.sender(), message.payload())));
    }

    @Override
    public void registerHandler(final String type, final BiFunction<Endpoint, byte[], byte[]> handler, final Executor executor) {
        handlers.put(type, (message, connection) -> executor.execute(() -> {
            byte[] responsePayload = null;
            InternalReply.Status status = InternalReply.Status.OK;
            try {
                responsePayload = handler.apply(message.sender(), message.payload());
            } catch (final Exception e) {
                LOGGER.debug("An error occurred in a message handler: {}", e);
                status = InternalReply.Status.ERROR_HANDLER_EXCEPTION;
            }
            connection.reply(message, status, Optional.ofNullable(responsePayload));
        }));
    }

    @Override
    public void registerHandler(final String type, final BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> handler) {
        handlers.put(type, (message, connection) -> {
            handler.apply(message.sender(), message.payload()).whenComplete((result, error) -> {
                final InternalReply.Status status;
                if (error == null) {
                    status = InternalReply.Status.OK;
                } else {
                    LOGGER.debug("An error occurred in a message handler: {}", error);
                    status = InternalReply.Status.ERROR_HANDLER_EXCEPTION;
                }
                connection.reply(message, status, Optional.ofNullable(result));
            });
        });
    }

    @Override
    public void unregisterHandler(final String type) {
        handlers.remove(type);
    }

    private List<CompletableFuture<Channel>> getChannelPool(final Endpoint endpoint) {
        return channels.computeIfAbsent(endpoint, e -> {
            final List<CompletableFuture<Channel>> defaultList = new ArrayList<>(CHANNEL_POOL_SIZE);
            for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
                defaultList.add(null);
            }
            return Lists.newCopyOnWriteArrayList(defaultList);
        });
    }

    private static int getChannelOffset(final String messageType) {
        return Math.abs(messageType.hashCode() % CHANNEL_POOL_SIZE);
    }

    private CompletableFuture<Channel> getChannel(final Endpoint endpoint, final String messageType) {
        final List<CompletableFuture<Channel>> channelPool = getChannelPool(endpoint);
        final int offset = getChannelOffset(messageType);

        CompletableFuture<Channel> channelFuture = channelPool.get(offset);
        if (channelFuture == null || channelFuture.isCompletedExceptionally()) {
            synchronized (channelPool) {
                channelFuture = channelPool.get(offset);
                if (channelFuture == null || channelFuture.isCompletedExceptionally()) {
                    channelFuture = openChannel(endpoint);
                    channelPool.set(offset, channelFuture);
                }
            }
        }

        final CompletableFuture<Channel> future = new CompletableFuture<>();
        final CompletableFuture<Channel> finalFuture = channelFuture;
        finalFuture.whenComplete((channel, error) -> {
            if (error == null) {
                if (!channel.isActive()) {
                    synchronized (channelPool) {
                        final CompletableFuture<Channel> currentFuture = channelPool.get(offset);
                        if (currentFuture == finalFuture) {
                            channelPool.set(offset, null);
                            getChannel(endpoint, messageType).whenComplete((recursiveResult, recursiveError) -> {
                                if (recursiveError == null) {
                                    future.complete(recursiveResult);
                                } else {
                                    future.completeExceptionally(recursiveError);
                                }
                            });
                        } else {
                            currentFuture.whenComplete((recursiveResult, recursiveError) -> {
                                if (recursiveError == null) {
                                    future.complete(recursiveResult);
                                } else {
                                    future.completeExceptionally(recursiveError);
                                }
                            });
                        }
                    }
                } else {
                    future.complete(channel);
                }
            } else {
                future.completeExceptionally(error);
            }
        });
        return future;
    }

    private <T> CompletableFuture<T> executeOnPooledConnection(
        final Endpoint endpoint,
        final String type,
        final Function<NettyMessagingService.ClientConnection, CompletableFuture<T>> callback,
        final Executor executor) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        executeOnPooledConnection(endpoint, type, callback, executor, future);
        return future;
    }

    private <T> void executeOnPooledConnection(
        final Endpoint endpoint,
        final String type,
        final Function<NettyMessagingService.ClientConnection, CompletableFuture<T>> callback,
        final Executor executor,
        final CompletableFuture<T> future) {
        if (endpoint.equals(localEndpoint)) {
            callback.apply(localClientConnection).whenComplete((result, error) -> {
                if (error == null) {
                    executor.execute(() -> future.complete(result));
                } else {
                    executor.execute(() -> future.completeExceptionally(error));
                }
            });
            return;
        }

        getChannel(endpoint, type).whenComplete((channel, channelError) -> {
            if (channelError == null) {
                final NettyMessagingService.ClientConnection connection = clientConnections.computeIfAbsent(channel, NettyMessagingService.RemoteClientConnection::new);
                callback.apply(connection).whenComplete((result, sendError) -> {
                    if (sendError == null) {
                        executor.execute(() -> future.complete(result));
                    } else {
                        executor.execute(() -> future.completeExceptionally(sendError));
                    }
                });
            } else {
                executor.execute(() -> future.completeExceptionally(channelError));
            }
        });
    }

    private Bootstrap bootstrapClient(final Endpoint endpoint) {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK,
            new WriteBufferWaterMark(10 * 32 * 1024, 10 * 64 * 1024));
        bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000);
        bootstrap.group(clientGroup);
        // TODO: Make this faster:
        // http://normanmaurer.me/presentations/2014-facebook-eng-netty/slides.html#37.0
        bootstrap.channel(clientChannelClass);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.remoteAddress(endpoint.host(), endpoint.port());
        if (enableNettyTls) {
            bootstrap.handler(new SslClientCommunicationChannelInitializer());
        } else {
            bootstrap.handler(new BasicChannelInitializer());
        }
        return bootstrap;
    }

    private CompletableFuture<Channel> openChannel(final Endpoint ep) {
        final Bootstrap bootstrap = bootstrapClient(ep);
        final CompletableFuture<Channel> retFuture = new CompletableFuture<>();
        final ChannelFuture f = bootstrap.connect();
        f.addListener(future -> {
            if (future.isSuccess()) {
                retFuture.complete(f.channel());
            } else {
                retFuture.completeExceptionally(future.cause());
            }
        });
        LOGGER.debug("Established a new connection to {}", ep);
        return retFuture;
    }

    /**
     * Represents the client side of a connection to a local or remote server.
     */
    private interface ClientConnection {

        /**
         * Sends a message to the other side of the connection.
         * @param message the message to send
         * @return a completable future to be completed once the message has been sent
         */
        CompletableFuture<Void> sendAsync(InternalRequest message);

        /**
         * Sends a message to the other side of the connection, awaiting a reply.
         * @param message the message to send
         * @return a completable future to be completed once a reply is received or the request times out
         */
        CompletableFuture<byte[]> sendAndReceive(InternalRequest message);

        /**
         * Closes the connection.
         */
        default void close() {
        }
    }

    /**
     * Represents the server side of a connection.
     */
    private interface ServerConnection {

        /**
         * Sends a reply to the other side of the connection.
         * @param message the message to which to reply
         * @param status the reply status
         * @param payload the response payload
         */
        void reply(InternalRequest message, InternalReply.Status status, Optional<byte[]> payload);

        /**
         * Closes the connection.
         */
        default void close() {
        }
    }

    /**
     * Netty messaging service builder.
     */
    public static class Builder extends MessagingService.Builder {
        private String name = DEFAULT_NAME;
        private Endpoint endpoint;

        /**
         * Sets the cluster name.
         * @param name the cluster name
         * @return the Netty messaging service builder
         * @throws NullPointerException if the name is null
         */
        public NettyMessagingService.Builder withName(final String name) {
            this.name = Preconditions.checkNotNull(name);
            return this;
        }

        /**
         * Sets the messaging endpoint.
         * @param endpoint the messaging endpoint
         * @return the Netty messaging service builder
         * @throws NullPointerException if the endpoint is null
         */
        public NettyMessagingService.Builder withEndpoint(final Endpoint endpoint) {
            this.endpoint = Preconditions.checkNotNull(endpoint);
            return this;
        }

        @Override
        public ManagedMessagingService build() {
            if (endpoint == null) {
                try {
                    endpoint = new Endpoint(InetAddress.getByName("127.0.0.1"), DEFAULT_PORT);
                } catch (final UnknownHostException e) {
                    throw new IllegalStateException("Failed to instantiate address", e);
                }
            }
            return new NettyMessagingService(name.hashCode(), endpoint);
        }
    }

    /**
     * Request-reply timeout history tracker.
     */
    private static final class TimeoutHistory {
        private final DescriptiveStatistics timeoutHistory = new SynchronizedDescriptiveStatistics(WINDOW_SIZE);
        private final AtomicLong maxReplyTime = new AtomicLong();
        private volatile long currentTimeout = DEFAULT_TIMEOUT_MILLIS;

        /**
         * Adds a reply time to the history.
         * @param replyTime the reply time to add to the history
         */
        void addReplyTime(final long replyTime) {
            maxReplyTime.getAndAccumulate(replyTime, Math::max);
        }

        /**
         * Computes the current timeout.
         */
        private void recomputeTimeoutMillis() {
            final double nextTimeout = (double) maxReplyTime.getAndSet(0L) * TIMEOUT_MULTIPLIER;
            timeoutHistory.addValue(
                Math.min(Math.max(nextTimeout, (double) MIN_TIMEOUT_MILLIS), (double) MAX_TIMEOUT_MILLIS));
            if (timeoutHistory.getN() == (long) WINDOW_SIZE) {
                this.currentTimeout = (long) timeoutHistory.getMax();
            }
        }
    }

    /**
     * Channel initializer for TLS servers.
     */
    private class SslServerCommunicationChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final ChannelHandler dispatcher = new InboundMessageDispatcher();

        @Override
        protected void initChannel(final SocketChannel channel) throws Exception {
            final SSLContext serverContext = SSLContext.getInstance("TLS");
            serverContext.init(keyManager.getKeyManagers(), trustManager.getTrustManagers(), null);

            final SSLEngine serverSslEngine = serverContext.createSSLEngine();

            serverSslEngine.setNeedClientAuth(true);
            serverSslEngine.setUseClientMode(false);
            serverSslEngine.setEnabledProtocols(serverSslEngine.getSupportedProtocols());
            serverSslEngine.setEnabledCipherSuites(serverSslEngine.getSupportedCipherSuites());
            serverSslEngine.setEnableSessionCreation(true);

            channel.pipeline().addLast("ssl", new SslHandler(serverSslEngine))
                .addLast("encoder", new MessageEncoder(localEndpoint, preamble))
                .addLast("decoder", new MessageDecoder())
                .addLast("handler", dispatcher);
        }
    }

    /**
     * Channel initializer for TLS clients.
     */
    private class SslClientCommunicationChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final ChannelHandler dispatcher = new InboundMessageDispatcher();

        @Override
        protected void initChannel(final SocketChannel channel) throws Exception {
            final SSLContext clientContext = SSLContext.getInstance("TLS");
            clientContext.init(keyManager.getKeyManagers(), trustManager.getTrustManagers(), null);

            final SSLEngine clientSslEngine = clientContext.createSSLEngine();

            clientSslEngine.setUseClientMode(true);
            clientSslEngine.setEnabledProtocols(clientSslEngine.getSupportedProtocols());
            clientSslEngine.setEnabledCipherSuites(clientSslEngine.getSupportedCipherSuites());
            clientSslEngine.setEnableSessionCreation(true);

            channel.pipeline().addLast("ssl", new SslHandler(clientSslEngine))
                .addLast("encoder", new MessageEncoder(localEndpoint, preamble))
                .addLast("decoder", new MessageDecoder())
                .addLast("handler", dispatcher);
        }
    }

    /**
     * Channel initializer for basic connections.
     */
    private class BasicChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final ChannelHandler dispatcher = new InboundMessageDispatcher();

        @Override
        protected void initChannel(final SocketChannel channel) {
            channel.pipeline()
                .addLast("encoder", new MessageEncoder(localEndpoint, preamble))
                .addLast("decoder", new MessageDecoder())
                .addLast("handler", dispatcher);
        }
    }

    /**
     * Channel inbound handler that dispatches messages to the appropriate handler.
     */
    @ChannelHandler.Sharable
    private class InboundMessageDispatcher extends SimpleChannelInboundHandler<Object> {
        // Effectively SimpleChannelInboundHandler<InternalMessage>,
        // had to specify <Object> to avoid Class Loader not being able to find some classes.

        @Override
        public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
            LOGGER.error("Exception inside channel handling pipeline.", cause);

            final NettyMessagingService.RemoteClientConnection clientConnection = clientConnections.remove(context.channel());
            if (clientConnection != null) {
                clientConnection.close();
            }

            final NettyMessagingService.RemoteServerConnection serverConnection = serverConnections.remove(context.channel());
            if (serverConnection != null) {
                serverConnection.close();
            }
            context.close();
        }

        /**
         * Returns true if the given message should be handled.
         * @param msg inbound message
         * @return true if {@code msg} is {@link InternalMessage} instance.
         * @see SimpleChannelInboundHandler#acceptInboundMessage(Object)
         */
        @Override
        public final boolean acceptInboundMessage(final Object msg) {
            return msg instanceof InternalMessage;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final Object rawMessage) {
            final InternalMessage message = (InternalMessage) rawMessage;
            try {
                if (message.isRequest()) {
                    final NettyMessagingService.RemoteServerConnection connection =
                        serverConnections.computeIfAbsent(ctx.channel(), NettyMessagingService.RemoteServerConnection::new);
                    connection.dispatch((InternalRequest) message);
                } else {
                    final NettyMessagingService.RemoteClientConnection connection =
                        clientConnections.computeIfAbsent(ctx.channel(), NettyMessagingService.RemoteClientConnection::new);
                    connection.dispatch((InternalReply) message);
                }
            } catch (final RejectedExecutionException e) {
                LOGGER.warn("Unable to dispatch message due to {}", e.getMessage());
            }
        }
    }

    /**
     * Wraps a {@link CompletableFuture} and tracks its type and creation time.
     */
    private static final class Callback {
        private final String type;
        private final CompletableFuture<byte[]> future;
        private final long time = System.currentTimeMillis();

        Callback(final String type, final CompletableFuture<byte[]> future) {
            this.type = type;
            this.future = future;
        }

        public void complete(final byte[] value) {
            future.complete(value);
        }

        public void completeExceptionally(final Throwable error) {
            future.completeExceptionally(error);
        }
    }

    /**
     * Local connection implementation.
     */
    private final class LocalClientConnection implements NettyMessagingService.ClientConnection {
        @Override
        public CompletableFuture<Void> sendAsync(final InternalRequest message) {
            final BiConsumer<InternalRequest, NettyMessagingService.ServerConnection> handler = handlers.get(message.subject());
            if (handler != null) {
                handler.accept(message, localServerConnection);
            } else {
                LOGGER.debug("No handler for message type {} from {}", message.type(), message.sender());
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<byte[]> sendAndReceive(final InternalRequest message) {
            final CompletableFuture<byte[]> future = new CompletableFuture<>();
            final BiConsumer<InternalRequest, NettyMessagingService.ServerConnection> handler = handlers.get(message.subject());
            if (handler != null) {
                handler.accept(message, new NettyMessagingService.LocalServerConnection(future));
            } else {
                LOGGER.warn("No handler for message type {} from {}", message.type(), message.sender());
                new NettyMessagingService.LocalServerConnection(future)
                    .reply(message, InternalReply.Status.ERROR_NO_HANDLER, Optional.empty());
            }
            return future;
        }
    }

    /**
     * Local server connection.
     */
    private static final class LocalServerConnection implements NettyMessagingService.ServerConnection {
        private final CompletableFuture<byte[]> future;

        LocalServerConnection(final CompletableFuture<byte[]> future) {
            this.future = future;
        }

        @Override
        public void reply(final InternalRequest message, final InternalReply.Status status, final Optional<byte[]> payload) {
            if (future != null) {
                if (status == InternalReply.Status.OK) {
                    future.complete(payload.orElse(EMPTY_PAYLOAD));
                } else if (status == InternalReply.Status.ERROR_NO_HANDLER) {
                    future.completeExceptionally(new MessagingException.NoRemoteHandler());
                } else if (status == InternalReply.Status.ERROR_HANDLER_EXCEPTION) {
                    future.completeExceptionally(new MessagingException.RemoteHandlerFailure());
                } else if (status == InternalReply.Status.PROTOCOL_EXCEPTION) {
                    future.completeExceptionally(new MessagingException.ProtocolException());
                }
            }
        }
    }

    /**
     * Remote connection implementation.
     */
    private final class RemoteClientConnection implements NettyMessagingService.ClientConnection {
        private final Channel channel;
        private final Map<Long, NettyMessagingService.Callback> futures = Maps.newConcurrentMap();
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Cache<String, NettyMessagingService.TimeoutHistory> timeoutHistories = CacheBuilder.newBuilder()
            .expireAfterAccess(HISTORY_EXPIRE_MILLIS, TimeUnit.MILLISECONDS)
            .build();

        RemoteClientConnection(final Channel channel) {
            this.channel = channel;
        }

        /**
         * Times out callbacks for this connection.
         */
        private void timeoutCallbacks() {
            // Store the current time.
            final long currentTime = System.currentTimeMillis();

            // Iterate through future callbacks and time out callbacks that have been alive
            // longer than the current timeout according to the message type.
            final Iterator<Map.Entry<Long, NettyMessagingService.Callback>> iterator = futures.entrySet().iterator();
            while (iterator.hasNext()) {
                final NettyMessagingService.Callback callback = iterator.next().getValue();
                try {
                    final NettyMessagingService.TimeoutHistory timeoutHistory = timeoutHistories.get(callback.type, NettyMessagingService.TimeoutHistory::new);
                    final long currentTimeout = timeoutHistory.currentTimeout;
                    if (currentTime - callback.time > currentTimeout) {
                        iterator.remove();
                        final long elapsedTime = currentTime - callback.time;
                        timeoutHistory.addReplyTime(elapsedTime);
                        callback.completeExceptionally(
                            new TimeoutException("Request timed out in " + elapsedTime + " milliseconds"));
                    }
                } catch (final ExecutionException e) {
                    throw new AssertionError();
                }
            }

            // Iterate through all timeout histories and recompute the timeout.
            for (final NettyMessagingService.TimeoutHistory timeoutHistory : timeoutHistories.asMap().values()) {
                timeoutHistory.recomputeTimeoutMillis();
            }
        }

        /**
         * Dispatches a message to a local handler.
         * @param message the message to dispatch
         */
        private void dispatch(final InternalReply message) {
            if (message.preamble() != preamble) {
                LOGGER.warn("Received {} with invalid preamble", message.type());
                return;
            }

            final NettyMessagingService.Callback callback = futures.remove(message.id());
            if (callback != null) {
                if (message.status() == InternalReply.Status.OK) {
                    callback.complete(message.payload());
                } else if (message.status() == InternalReply.Status.ERROR_NO_HANDLER) {
                    callback.completeExceptionally(new MessagingException.NoRemoteHandler());
                } else if (message.status() == InternalReply.Status.ERROR_HANDLER_EXCEPTION) {
                    callback.completeExceptionally(new MessagingException.RemoteHandlerFailure());
                } else if (message.status() == InternalReply.Status.PROTOCOL_EXCEPTION) {
                    callback.completeExceptionally(new MessagingException.ProtocolException());
                }

                try {
                    final NettyMessagingService.TimeoutHistory timeoutHistory = timeoutHistories.get(callback.type, NettyMessagingService.TimeoutHistory::new);
                    timeoutHistory.addReplyTime(System.currentTimeMillis() - callback.time);
                } catch (final ExecutionException e) {
                    throw new AssertionError();
                }
            } else {
                LOGGER.warn("Received a reply for message id:[{}] "
                    + "but was unable to locate the"
                    + " request handle", message.id());
            }
        }

        @Override
        public CompletableFuture<Void> sendAsync(final InternalRequest message) {
            final CompletableFuture<Void> future = new CompletableFuture<>();
            channel.writeAndFlush(message).addListener(channelFuture -> {
                if (channelFuture.isSuccess()) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(channelFuture.cause());
                }
            });
            return future;
        }

        @Override
        public CompletableFuture<byte[]> sendAndReceive(final InternalRequest message) {
            final CompletableFuture<byte[]> future = new CompletableFuture<>();
            final NettyMessagingService.Callback callback = new NettyMessagingService.Callback(message.subject(), future);
            futures.put(message.id(), callback);
            channel.writeAndFlush(message).addListener(channelFuture -> {
                if (!channelFuture.isSuccess()) {
                    futures.remove(message.id());
                    callback.completeExceptionally(channelFuture.cause());
                }
            });
            return future;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                timeoutFuture.cancel(false);
                for (final NettyMessagingService.Callback callback : futures.values()) {
                    callback.completeExceptionally(new ConnectException());
                }
            }
        }
    }

    /**
     * Remote server connection.
     */
    private final class RemoteServerConnection implements NettyMessagingService.ServerConnection {
        private final Channel channel;

        RemoteServerConnection(final Channel channel) {
            this.channel = channel;
        }

        /**
         * Dispatches a message to a local handler.
         * @param message the message to dispatch
         */
        private void dispatch(final InternalRequest message) {
            if (message.preamble() != preamble) {
                LOGGER.warn("Received {} with invalid preamble from {}", message.type(), message.sender());
                reply(message, InternalReply.Status.PROTOCOL_EXCEPTION, Optional.empty());
                return;
            }

            final BiConsumer<InternalRequest, NettyMessagingService.ServerConnection> handler = handlers.get(message.subject());
            if (handler != null) {
                handler.accept(message, this);
            } else {
                LOGGER.warn("No handler for message type {} from {}", message.type(), message.sender());
                reply(message, InternalReply.Status.ERROR_NO_HANDLER, Optional.empty());
            }
        }

        @Override
        public void reply(final InternalRequest message, final InternalReply.Status status, final Optional<byte[]> payload) {
            final InternalReply response = new InternalReply(preamble,
                message.id(),
                payload.orElse(EMPTY_PAYLOAD),
                status);
            channel.writeAndFlush(response);
        }
    }
}
