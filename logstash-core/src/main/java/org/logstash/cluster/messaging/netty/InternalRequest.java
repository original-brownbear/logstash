package org.logstash.cluster.messaging.netty;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.messaging.Endpoint;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * Internal request message.
 */
public final class InternalRequest extends InternalMessage {
    private final Endpoint sender;
    private final String subject;

    public InternalRequest(
        int preamble,
        long id,
        Endpoint sender,
        String subject,
        byte[] payload) {
        super(preamble, id, payload);
        this.sender = sender;
        this.subject = subject;
    }

    @Override
    public InternalMessage.Type type() {
        return InternalMessage.Type.REQUEST;
    }

    public String subject() {
        return subject;
    }

    public Endpoint sender() {
        return sender;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id())
            .add("subject", subject)
            .add("sender", sender)
            .add("payload", ArraySizeHashPrinter.of(payload()))
            .toString();
    }
}
