/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.logstash.cluster.utils.concurrent;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import org.logstash.cluster.utils.Match;

/**
 * Utilities for creating completed and exceptional futures.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class Futures {

    /**
     * Creates a future that is synchronously completed.
     * @param result The future result.
     * @return The completed future.
     */
    public static <T> CompletableFuture<T> completedFuture(final T result) {
        return CompletableFuture.completedFuture(result);
    }

    /**
     * Creates a future that is asynchronously completed.
     * @param result The future result.
     * @param executor The executor on which to complete the future.
     * @return The completed future.
     */
    public static <T> CompletableFuture<T> completedFutureAsync(final T result, final Executor executor) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        executor.execute(() -> future.complete(result));
        return future;
    }

    /**
     * Creates a future that is synchronously completed exceptionally.
     * @param t The future exception.
     * @return The exceptionally completed future.
     */
    public static <T> CompletableFuture<T> exceptionalFuture(final Throwable t) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    /**
     * Creates a future that is asynchronously completed exceptionally.
     * @param t The future exception.
     * @param executor The executor on which to complete the future.
     * @return The exceptionally completed future.
     */
    public static <T> CompletableFuture<T> exceptionalFutureAsync(final Throwable t, final Executor executor) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        executor.execute(() -> {
            future.completeExceptionally(t);
        });
        return future;
    }

    /**
     * Returns a future that completes callbacks in add order.
     * @param <T> future value type
     * @return a new completable future that will complete added callbacks in the order in which they were added
     */
    public static <T> CompletableFuture<T> orderedFuture() {
        return new OrderedFuture<>();
    }

    /**
     * Returns a wrapped future that will be completed on the given executor.
     * @param future the future to be completed on the given executor
     * @param executor the executor with which to complete the future
     * @param <T> the future value type
     * @return a wrapped future to be completed on the given executor
     */
    public static <T> CompletableFuture<T> asyncFuture(final CompletableFuture<T> future, final Executor executor) {
        final CompletableFuture<T> newFuture = new CompletableFuture<>();
        future.whenComplete((result, error) -> {
            executor.execute(() -> {
                if (error == null) {
                    newFuture.complete(result);
                } else {
                    newFuture.completeExceptionally(error);
                }
            });
        });
        return newFuture;
    }

    /**
     * Returns a future that's completed using the given {@code orderedExecutor} if the future is not blocked or the
     * given {@code threadPoolExecutor} if the future is blocked.
     * <p>
     * This method allows futures to maintain single-thread semantics via the provided {@code orderedExecutor} while
     * ensuring user code can block without blocking completion of futures. When the returned future or any of its
     * descendants is blocked on a {@link CompletableFuture#get()} or {@link CompletableFuture#join()} call, completion
     * of the returned future will be done using the provided {@code threadPoolExecutor}.
     * @param future the future to convert into an asynchronous future
     * @param executor the executor with which to attempt to complete the future
     * @param <T> future value type
     * @return a new completable future to be completed using the provided {@code executor} once the provided
     * {@code future} is complete
     */
    public static <T> CompletableFuture<T> blockingAwareFuture(final CompletableFuture<T> future, final Executor executor) {
        if (future.isDone()) {
            return future;
        }

        final BlockingAwareFuture<T> newFuture = new BlockingAwareFuture<>();
        future.whenComplete((result, error) -> {
            if (newFuture.isBlocked()) {
                if (error == null) {
                    newFuture.complete(result);
                } else {
                    newFuture.completeExceptionally(error);
                }
            } else {
                if (error == null) {
                    executor.execute(() -> newFuture.complete(result));
                } else {
                    executor.execute(() -> newFuture.completeExceptionally(error));
                }
            }
        });
        return newFuture;
    }

    /**
     * Returns a new CompletableFuture completed by reducing a list of computed values
     * when all of the given CompletableFuture complete.
     * @param futures the CompletableFutures
     * @param reducer reducer for computing the result
     * @param emptyValue zero value to be returned if the input future list is empty
     * @param <T> value type of CompletableFuture
     * @return a new CompletableFuture that is completed when all of the given CompletableFutures complete
     */
    public static <T> CompletableFuture<T> allOf(final List<CompletableFuture<T>> futures,
        final BinaryOperator<T> reducer,
        final T emptyValue) {
        return allOf(futures).thenApply(resultList -> resultList.stream().reduce(reducer).orElse(emptyValue));
    }

    /**
     * Returns a new CompletableFuture completed with a list of computed values
     * when all of the given CompletableFuture complete.
     * @param futures the CompletableFutures
     * @param <T> value type of CompletableFuture
     * @return a new CompletableFuture that is completed when all of the given CompletableFutures complete
     */
    public static <T> CompletableFuture<List<T>> allOf(final List<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList())
            );
    }

    /**
     * Returns a new CompletableFuture completed by with the first positive result from a list of
     * input CompletableFutures.
     * @param futures the input list of CompletableFutures
     * @param positiveResultMatcher matcher to identify a positive result
     * @param negativeResult value to complete with if none of the futures complete with a positive result
     * @param <T> value type of CompletableFuture
     * @return a new CompletableFuture
     */
    public static <T> CompletableFuture<T> firstOf(final List<CompletableFuture<T>> futures,
        final Match<T> positiveResultMatcher,
        final T negativeResult) {
        final CompletableFuture<T> responseFuture = new CompletableFuture<>();
        allOf(Lists.transform(futures, future -> future.thenAccept(r -> {
            if (positiveResultMatcher.matches(r)) {
                responseFuture.complete(r);
            }
        }))).whenComplete((r, e) -> {
            if (!responseFuture.isDone()) {
                if (e != null) {
                    responseFuture.completeExceptionally(e);
                } else {
                    responseFuture.complete(negativeResult);
                }
            }
        });
        return responseFuture;
    }

}