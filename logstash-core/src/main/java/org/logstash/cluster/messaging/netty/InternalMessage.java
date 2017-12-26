package org.logstash.cluster.messaging.netty;

/**
 * Base class for internal messages.
 */
public abstract class InternalMessage {

    private final int preamble;
    private final long id;
    private final byte[] payload;
    protected InternalMessage(int preamble,
        long id,
        byte[] payload) {
        this.preamble = preamble;
        this.id = id;
        this.payload = payload;
    }

    public boolean isRequest() {
        return type() == InternalMessage.Type.REQUEST;
    }

    public abstract InternalMessage.Type type();

    public boolean isReply() {
        return type() == InternalMessage.Type.REPLY;
    }

    public int preamble() {
        return preamble;
    }

    public long id() {
        return id;
    }

    public byte[] payload() {
        return payload;
    }

    /**
     * Internal message type.
     */
    public enum Type {
        REQUEST(1),
        REPLY(2);

        private final int id;

        Type(int id) {
            this.id = id;
        }

        /**
         * Returns the message type enum associated with the given ID.
         * @param id the type ID.
         * @return the type enum for the given ID.
         */
        public static InternalMessage.Type forId(int id) {
            switch (id) {
                case 1:
                    return REQUEST;
                case 2:
                    return REPLY;
                default:
                    throw new IllegalArgumentException("Unknown status ID " + id);
            }
        }

        /**
         * Returns the unique message type ID.
         * @return the unique message type ID.
         */
        public int id() {
            return id;
        }
    }
}
