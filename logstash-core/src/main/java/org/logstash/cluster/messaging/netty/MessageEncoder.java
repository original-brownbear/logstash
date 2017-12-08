package org.logstash.cluster.messaging.netty;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.io.IOException;
import java.net.InetAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.messaging.Endpoint;

/**
 * Encode InternalMessage out into a byte buffer.
 */
public class MessageEncoder extends MessageToByteEncoder<Object> {
// Effectively MessageToByteEncoder<InternalMessage>,
// had to specify <Object> to avoid Class Loader not being able to find some classes.

    private static final Logger LOGGER = LogManager.getLogger(MessageEncoder.class);

    private final Endpoint endpoint;
    private final int preamble;
    private boolean endpointWritten;

    public MessageEncoder(Endpoint endpoint, int preamble) {
        super();
        this.endpoint = endpoint;
        this.preamble = preamble;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        if (cause instanceof IOException) {
            LOGGER.debug("IOException inside channel handling pipeline.", cause);
        } else {
            LOGGER.error("non-IOException inside channel handling pipeline.", cause);
        }
        context.close();
    }

    // Effectively same result as one generated by MessageToByteEncoder<InternalMessage>
    @Override
    public final boolean acceptOutboundMessage(Object msg) {
        return msg instanceof InternalMessage;
    }

    @Override
    protected void encode(ChannelHandlerContext context, Object rawMessage, ByteBuf out) {
        if (rawMessage instanceof InternalRequest) {
            encodeRequest((InternalRequest) rawMessage, out);
        } else if (rawMessage instanceof InternalReply) {
            encodeReply((InternalReply) rawMessage, out);
        }
    }

    private void encodeRequest(InternalRequest request, ByteBuf out) {
        encodeMessage(request, out);

        byte[] messageTypeBytes = request.subject().getBytes(Charsets.UTF_8);

        // write length of message type
        out.writeShort(messageTypeBytes.length);

        // write message type bytes
        out.writeBytes(messageTypeBytes);

    }

    private void encodeMessage(InternalMessage message, ByteBuf out) {
        // If the endpoint hasn't been written to the channel, write it.
        if (!endpointWritten) {
            InetAddress senderIp = endpoint.host();
            byte[] senderIpBytes = senderIp.getAddress();
            out.writeByte(senderIpBytes.length);
            out.writeBytes(senderIpBytes);

            // write sender port
            out.writeInt(endpoint.port());

            endpointWritten = true;
        }

        out.writeByte(message.type().id());
        out.writeInt(this.preamble);

        // write message id
        out.writeLong(message.id());

        byte[] payload = message.payload();

        // write payload length
        out.writeInt(payload.length);

        // write payload.
        out.writeBytes(payload);
    }

    private void encodeReply(InternalReply reply, ByteBuf out) {
        encodeMessage(reply, out);

        // write message status value
        out.writeByte(reply.status().id());
    }
}