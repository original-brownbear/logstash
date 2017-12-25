package org.logstash.cluster.primitives;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous primitive.
 */
public interface AsyncPrimitive extends DistributedPrimitive {

    /**
     * Purges state associated with this primitive.
     * <p>
     * Implementations can override and provide appropriate clean up logic for purging
     * any state state associated with the primitive. Whether modifications made within the
     * destroy method have local or global visibility is left unspecified.
     * @return {@code CompletableFuture} that is completed when the operation completes
     */
    default CompletableFuture<Void> destroy() {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Closes the primitive.
     * @return a future to be completed once the primitive is closed
     */
    CompletableFuture<Void> close();

}
