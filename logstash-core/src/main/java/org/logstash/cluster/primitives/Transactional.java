package org.logstash.cluster.primitives;

import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.time.Version;

/**
 * Interface for transactional primitives.
 */
public interface Transactional<T> {

    /**
     * Begins the transaction.
     * @param transactionId the transaction identifier for the transaction to begin
     * @return a completable future to be completed with the lock version
     */
    CompletableFuture<Version> begin(TransactionId transactionId);

    /**
     * Prepares a transaction for commitment.
     * @param transactionLog transaction log
     * @return {@code true} if prepare is successful and transaction is ready to be committed
     * {@code false} otherwise
     */
    CompletableFuture<Boolean> prepare(TransactionLog<T> transactionLog);

    /**
     * Prepares and commits a transaction.
     * @param transactionLog transaction log
     * @return {@code true} if prepare is successful and transaction was committed
     * {@code false} otherwise
     */
    CompletableFuture<Boolean> prepareAndCommit(TransactionLog<T> transactionLog);

    /**
     * Commits a previously prepared transaction and unlocks the object.
     * @param transactionId transaction identifier
     * @return future that will be completed when the operation finishes
     */
    CompletableFuture<Void> commit(TransactionId transactionId);

    /**
     * Aborts a previously prepared transaction and unlocks the object.
     * @param transactionId transaction identifier
     * @return future that will be completed when the operation finishes
     */
    CompletableFuture<Void> rollback(TransactionId transactionId);

}
