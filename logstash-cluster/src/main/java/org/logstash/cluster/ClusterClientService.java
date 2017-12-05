package org.logstash.cluster;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ClusterClientService implements LsClusterService {

    private static final Logger LOGGER = LogManager.getLogger(ClusterClientService.class);

    private final CountDownLatch stopped = new CountDownLatch(1);

    private final EventLoopGroup boss;
    private final EventLoopGroup worker;

    private final ClusterStateManagerService state;

    private final Collection<InetSocketAddress> connectedPeers = new HashSet<>();

    private final ChannelFuture server;

    public ClusterClientService(final ClusterStateManagerService state) {
        this(state, new InetSocketAddress(getDefaultBindAddress(), getDefaultBindPort()));
    }

    public ClusterClientService(final ClusterStateManagerService state,
        final InetSocketAddress address) {
        this.state = state;
        boss = new NioEventLoopGroup(1);
        worker = new NioEventLoopGroup(1);
        server = new ServerBootstrap().group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(final SocketChannel channel) {
                    channel.pipeline().addLast(new ClusterClientService.LsClusterChannelHandler());
                }
            })
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .bind(address);
    }

    @Override
    public void run() {
        try {
            while (!this.stopped.await(100L, TimeUnit.MILLISECONDS)) {
                final Collection<InetSocketAddress> outstanding = new HashSet<>(state.peers());
                outstanding.removeAll(connectedPeers);
            }
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void stop() {
        stopped.countDown();
    }

    @Override
    public void awaitStop() {
        try {
            stopped.await();
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void close() {
        stop();
        awaitStop();
        worker.shutdownGracefully().syncUninterruptibly();
        boss.shutdownGracefully().syncUninterruptibly();
        server.channel().closeFuture().syncUninterruptibly();
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

    private final class LsClusterChannelHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
            LOGGER.info("Received connection from {}", remote);
            state.registerPeer(remote);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ((ByteBuf) msg).release();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOGGER.error(cause);
            ctx.close();
        }
    }
}
