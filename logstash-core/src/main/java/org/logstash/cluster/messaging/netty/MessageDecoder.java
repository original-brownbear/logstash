package org.logstash.cluster.messaging.netty;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import java.net.InetAddress;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.messaging.Endpoint;

/**
 * Decoder for inbound messages.
 */
public class MessageDecoder extends ReplayingDecoder<DecoderState> {

    private static final Logger log = LogManager.getLogger(MessageDecoder.class);

    private InetAddress senderIp;
    private int senderPort;

    private InternalMessage.Type type;
    private int preamble;
    private long messageId;
    private int contentLength;
    private byte[] content;
    private int subjectLength;
    private String subject;
    private InternalReply.Status status;

    public MessageDecoder() {
        super(DecoderState.READ_SENDER_IP);
    }

    @Override
    @SuppressWarnings("squid:S128") // suppress switch fall through warning
    protected void decode(
        ChannelHandlerContext context,
        ByteBuf buffer,
        List<Object> out) throws Exception {

        switch (state()) {
            case READ_SENDER_IP:
                byte[] octets = new byte[buffer.readByte()];
                buffer.readBytes(octets);
                senderIp = InetAddress.getByAddress(octets);
                checkpoint(DecoderState.READ_SENDER_PORT);
                checkpoint(DecoderState.READ_SENDER_PORT);
            case READ_SENDER_PORT:
                senderPort = buffer.readInt();
                checkpoint(DecoderState.READ_TYPE);
            case READ_TYPE:
                type = InternalMessage.Type.forId(buffer.readByte());
                checkpoint(DecoderState.READ_PREAMBLE);
            case READ_PREAMBLE:
                preamble = buffer.readInt();
                checkpoint(DecoderState.READ_MESSAGE_ID);
            case READ_MESSAGE_ID:
                messageId = buffer.readLong();
                checkpoint(DecoderState.READ_CONTENT_LENGTH);
            case READ_CONTENT_LENGTH:
                contentLength = buffer.readInt();
                checkpoint(DecoderState.READ_CONTENT);
            case READ_CONTENT:
                if (contentLength > 0) {
                    //TODO Perform a sanity check on the size before allocating
                    content = new byte[contentLength];
                    buffer.readBytes(content);
                } else {
                    content = new byte[0];
                }

                switch (type) {
                    case REQUEST:
                        checkpoint(DecoderState.READ_SUBJECT_LENGTH);
                        break;
                    case REPLY:
                        checkpoint(DecoderState.READ_STATUS);
                        break;
                    default:
                        Preconditions.checkState(false, "Must not be here");
                }
                break;
            default:
                break;
        }

        switch (type) {
            case REQUEST:
                switch (state()) {
                    case READ_SUBJECT_LENGTH:
                        subjectLength = buffer.readShort();
                        checkpoint(DecoderState.READ_SUBJECT);
                    case READ_SUBJECT:
                        byte[] messageTypeBytes = new byte[subjectLength];
                        buffer.readBytes(messageTypeBytes);
                        subject = new String(messageTypeBytes, Charsets.UTF_8);
                        InternalRequest message = new InternalRequest(
                            preamble,
                            messageId,
                            new Endpoint(senderIp, senderPort),
                            subject,
                            content);
                        out.add(message);
                        checkpoint(DecoderState.READ_TYPE);
                        break;
                    default:
                        break;
                }
                break;
            case REPLY:
                switch (state()) {
                    case READ_STATUS:
                        status = InternalReply.Status.forId(buffer.readByte());
                        InternalReply message = new InternalReply(preamble,
                            messageId,
                            content,
                            status);
                        out.add(message);
                        checkpoint(DecoderState.READ_TYPE);
                        break;
                    default:
                        break;
                }
                break;
            default:
                Preconditions.checkState(false, "Must not be here");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        log.error("Exception inside channel handling pipeline.", cause);
        context.close();
    }
}
