package org.logstash.cluster.messaging.netty;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * Internal reply message.
 */
public final class InternalReply extends InternalMessage {

    private final InternalReply.Status status;

    public InternalReply(int preamble,
        long id,
        InternalReply.Status status) {
        this(preamble, id, new byte[0], status);
    }

    public InternalReply(int preamble,
        long id,
        byte[] payload,
        InternalReply.Status status) {
        super(preamble, id, payload);
        this.status = status;
    }

    @Override
    public InternalMessage.Type type() {
        return InternalMessage.Type.REPLY;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id())
            .add("status", status())
            .add("payload", ArraySizeHashPrinter.of(payload()))
            .toString();
    }

    public InternalReply.Status status() {
        return status;
    }

    /**
     * Message status.
     */
    public enum Status {

        // NOTE: For backwards compatibility enum constant IDs should not be changed.

        /**
         * All ok.
         */
        OK(0),

        /**
         * Response status signifying no registered handler.
         */
        ERROR_NO_HANDLER(1),

        /**
         * Response status signifying an exception handling the message.
         */
        ERROR_HANDLER_EXCEPTION(2),

        /**
         * Response status signifying invalid message structure.
         */
        PROTOCOL_EXCEPTION(3);

        private final int id;

        Status(int id) {
            this.id = id;
        }

        /**
         * Returns the status enum associated with the given ID.
         * @param id the status ID.
         * @return the status enum for the given ID.
         */
        public static InternalReply.Status forId(int id) {
            switch (id) {
                case 0:
                    return OK;
                case 1:
                    return ERROR_NO_HANDLER;
                case 2:
                    return ERROR_HANDLER_EXCEPTION;
                case 3:
                    return PROTOCOL_EXCEPTION;
                default:
                    throw new IllegalArgumentException("Unknown status ID " + id);
            }
        }

        /**
         * Returns the unique status ID.
         * @return the unique status ID.
         */
        public int id() {
            return id;
        }
    }
}
