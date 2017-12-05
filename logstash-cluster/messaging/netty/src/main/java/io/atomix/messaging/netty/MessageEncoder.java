/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.messaging.netty;

import com.google.common.base.Charsets;
import io.atomix.messaging.Endpoint;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Encode InternalMessage out into a byte buffer.
 */
public class MessageEncoder extends MessageToByteEncoder<Object> {
// Effectively MessageToByteEncoder<InternalMessage>,
// had to specify <Object> to avoid Class Loader not being able to find some classes.

  private final Logger log = LoggerFactory.getLogger(getClass());

  private final Endpoint endpoint;
  private final int preamble;
  private boolean endpointWritten;

  public MessageEncoder(Endpoint endpoint, int preamble) {
    super();
    this.endpoint = endpoint;
    this.preamble = preamble;
  }

  @Override
  protected void encode(
      ChannelHandlerContext context,
      Object rawMessage,
      ByteBuf out) throws Exception {
    if (rawMessage instanceof InternalRequest) {
      encodeRequest((InternalRequest) rawMessage, out);
    } else if (rawMessage instanceof InternalReply) {
      encodeReply((InternalReply) rawMessage, out);
    }
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

  private void encodeRequest(InternalRequest request, ByteBuf out) {
    encodeMessage(request, out);

    byte[] messageTypeBytes = request.subject().getBytes(Charsets.UTF_8);

    // write length of message type
    out.writeShort(messageTypeBytes.length);

    // write message type bytes
    out.writeBytes(messageTypeBytes);

  }

  private void encodeReply(InternalReply reply, ByteBuf out) {
    encodeMessage(reply, out);

    // write message status value
    out.writeByte(reply.status().id());
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
    if (cause instanceof IOException) {
      log.debug("IOException inside channel handling pipeline.", cause);
    } else {
      log.error("non-IOException inside channel handling pipeline.", cause);
    }
    context.close();
  }

  // Effectively same result as one generated by MessageToByteEncoder<InternalMessage>
  @Override
  public final boolean acceptOutboundMessage(Object msg) throws Exception {
    return msg instanceof InternalMessage;
  }
}