package org.logstash.cluster.io;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ReplayingDecoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.List;
import org.logstash.cluster.raft.RaftMessage;

public final class RaftMessageNettyCodec {

    public static final class RaftMessageDecoder extends ReplayingDecoder<Void> {
        @Override
        protected void decode(
            final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) {
            final byte[] raw = new byte[in.readInt()];
            in.readBytes(raw);
            try (final ObjectInput objInput = new ObjectInputStream(new ByteArrayInputStream(raw))) {
                out.add(objInput.readObject());
            } catch (final IOException | ClassNotFoundException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    public static final class RaftMessageEncoder extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            RaftMessage m = (RaftMessage) msg;
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (final ObjectOutput objectOut = new ObjectOutputStream(baos)) {
                m.writeTo(objectOut);
            } catch (final IOException ex) {
                throw new IllegalStateException(ex);
            }
            final int len = baos.size();
            ByteBuf encoded = ctx.alloc().buffer(len + 4);
            encoded.writeInt(len);
            encoded.writeBytes(baos.toByteArray());
            baos.reset();
            ctx.write(encoded, promise);
        }
    }
}
