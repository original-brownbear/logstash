package org.logstash.cluster.primitives;

import org.logstash.cluster.utils.AbstractIdentifier;

/**
 * Transaction identifier.
 */
public final class TransactionId extends AbstractIdentifier<String> {

    private TransactionId(String id) {
        super(id);
    }

    /**
     * Creates a new transaction identifier.
     * @param id backing identifier value
     * @return transaction identifier
     */
    public static TransactionId from(String id) {
        return new TransactionId(id);
    }
}
