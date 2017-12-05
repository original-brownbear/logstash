package org.logstash.cluster;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.io.RaftMessageNettyCodec;
import org.logstash.cluster.raft.RaftRpcMessage;

public final class ClusterNetworkClientService implements LsClusterService {

    private static final Logger LOGGER = LogManager.getLogger(ClusterNetworkClientService.class);

    private final CountDownLatch stopped = new CountDownLatch(1);

    private final EventLoopGroup boss;

    private final EventLoopGroup worker;

    private final ClusterStateService state;

    private final HashMap<InetSocketAddress, ChannelFuture> connectedPeers = new HashMap<>();

    private final ChannelFuture server;

    private final Bootstrap clientBootstrap;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public ClusterNetworkClientService(final ClusterStateService state) {
        this(state, new InetSocketAddress(getDefaultBindAddress(), getDefaultBindPort()));
    }

    public ClusterNetworkClientService(final ClusterStateService state,
        final InetSocketAddress address) {
        executor.submit(state);
        this.state = state;
        boss = new NioEventLoopGroup();
        worker = new NioEventLoopGroup();
        clientBootstrap = new Bootstrap().group(worker).channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(final SocketChannel channel) {
                    channel.pipeline().addLast(
                        new RaftMessageNettyCodec.RaftMessageEncoder(),
                        new ClusterNetworkClientService.LsOutgoingClusterChannel(state, address)
                    );
                }
            });
        server = new ServerBootstrap().group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .childHandler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(final SocketChannel channel) {
                        channel.pipeline().addLast(
                            new RaftMessageNettyCodec.RaftMessageDecoder(),
                            new ClusterNetworkClientService.LsIncomingClusterChannel(state)
                        );
                    }
                })
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .bind(address);
        LOGGER.info("{} is listening on {}", state.getIdentifier(), address);
    }

    @Override
    public void run() {
        try {
            while (!this.stopped.await(100L, TimeUnit.MILLISECONDS)) {
                final Collection<InetSocketAddress> outstanding = new HashSet<>(state.peers());
                outstanding.removeAll(connectedPeers.keySet());
                for (final InetSocketAddress target : outstanding) {
                    LOGGER.info("{} connecting to {}", state.getIdentifier(), target);
                    clientBootstrap.connect(target).addListener(
                        future -> {
                            connectedPeers.put(target, clientBootstrap.connect(target));
                            LOGGER.info("{} connected to {}", state.getIdentifier(), target);
                        }
                    );
                }
            }
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        } finally {
            stopped.countDown();
        }
    }

    @Override
    public void stop() {
        state.stop();
        stopped.countDown();
    }

    @Override
    public void awaitStop() {
        try {
            state.awaitStop();
            stopped.await();
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void close() {
        stop();
        awaitStop();
        executor.shutdownNow();
        worker.shutdownGracefully().syncUninterruptibly();
        boss.shutdownGracefully().syncUninterruptibly();
        server.channel().closeFuture().syncUninterruptibly();
        try {
            executor.awaitTermination(2L, TimeUnit.MINUTES);
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static InetAddress getDefaultBindAddress() {
        try {
            return InetAddress.getByName(
                System.getProperty(
                    "logstash.bind.address", InetAddress.getLoopbackAddress().getHostAddress()
                )
            );
        } catch (final UnknownHostException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static int getDefaultBindPort() {
        return Integer.parseInt(System.getProperty("logstash.bind.port", "9700"));
    }

    private static final class LsOutgoingClusterChannel extends ChannelInboundHandlerAdapter {

        private final ClusterStateService state;

        private final InetSocketAddress address;

        private LsOutgoingClusterChannel(final ClusterStateService state,
            final InetSocketAddress address) {
            this.state = state;
            this.address = address;
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            ctx.writeAndFlush(new RaftRpcMessage(address, state.getTerm()));
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {

        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOGGER.error(cause);
            ctx.close();
        }
    }

    private static final class LsIncomingClusterChannel extends ChannelInboundHandlerAdapter {

        private final ClusterStateService state;

        private LsIncomingClusterChannel(final ClusterStateService state) {
            this.state = state;
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            final InetSocketAddress source = (InetSocketAddress) ctx.channel().remoteAddress();
            LOGGER.info("Incoming connection from {}", source);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            state.registerPeer(((RaftRpcMessage) msg).getSender());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOGGER.error(cause);
            ctx.close();
        }
    }
}
