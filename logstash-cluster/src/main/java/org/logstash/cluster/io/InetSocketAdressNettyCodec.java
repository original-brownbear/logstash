package org.logstash.cluster.io;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ReplayingDecoder;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;

public final class InetSocketAdressNettyCodec {

    public static final class InetSocketAdressDecoder extends ReplayingDecoder<Void> {
        @Override
        protected void decode(
            final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) {
            final byte[] raw = new byte[in.readInt()];
            in.readBytes(raw);
            try {
                out.add(new InetSocketAddress(InetAddress.getByAddress(raw), in.readInt()));
            } catch (final UnknownHostException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    public static final class InetSocketAdressEncoder extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            InetSocketAddress m = (InetSocketAddress) msg;
            final byte[] raw = m.getAddress().getAddress();
            ByteBuf encoded = ctx.alloc().buffer(raw.length + 4 + 4);
            encoded.writeInt(raw.length);
            encoded.writeBytes(raw);
            encoded.writeInt(m.getPort());
            ctx.write(encoded, promise);
        }
    }
}
