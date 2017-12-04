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
import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ClusterClientService implements Runnable, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(ClusterClientService.class);

    private final EventLoopGroup boss;
    private final EventLoopGroup worker;

    private final ClusterStateManager state;

    private final ChannelFuture server;

    public ClusterClientService(final ClusterStateManager state) {
        this(state, getDefaultBindAddress(), getDefaultBindPort());
    }

    public ClusterClientService(final ClusterStateManager state, final InetAddress host,
        final int port) {
        this.state = state;
        boss = new NioEventLoopGroup(1);
        worker = new NioEventLoopGroup(1);
        server = new ServerBootstrap().group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new ClusterClientService.LsClusterChannelHandler());
                }
            })
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .bind(host, port);
    }

    @Override
    public void run() {
        server.syncUninterruptibly();
    }

    @Override
    public void close() {
        worker.shutdownGracefully();
        boss.shutdownGracefully();
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

    private static final class LsClusterChannelHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            LOGGER.info("Received connection from {}", ctx.channel().remoteAddress());
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
