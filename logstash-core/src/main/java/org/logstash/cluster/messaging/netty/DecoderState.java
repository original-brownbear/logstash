package org.logstash.cluster.messaging.netty;

/**
 * State transitions a decoder goes through as it is decoding an incoming message.
 */
public enum DecoderState {
    READ_TYPE,
    READ_PREAMBLE,
    READ_MESSAGE_ID,
    READ_SENDER_IP,
    READ_SENDER_PORT,
    READ_SUBJECT_LENGTH,
    READ_SUBJECT,
    READ_STATUS,
    READ_CONTENT_LENGTH,
    READ_CONTENT
}
