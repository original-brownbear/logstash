/*
 * Copyright 2017-present Open Networking Foundation
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

import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Retry utilities.
 */
public final class Retries {
    private static final Random random = new Random();

    private Retries() {
    }

    /**
     * Returns a function that retries execution on failure.
     * @param base base function
     * @param exceptionClass type of exception for which to retry
     * @param maxRetries max number of retries before giving up
     * @param maxDelayBetweenRetries max delay between successive retries. The actual delay is randomly picked from
     * the interval (0, maxDelayBetweenRetries]
     * @param <U> type of function input
     * @param <V> type of function output
     * @return function
     */
    public static <U, V> Function<U, V> retryable(Function<U, V> base,
        Class<? extends Throwable> exceptionClass,
        int maxRetries,
        int maxDelayBetweenRetries) {
        return new RetryingFunction<>(base, exceptionClass, maxRetries, maxDelayBetweenRetries);
    }

    /**
     * Returns a Supplier that retries execution on failure.
     * @param base base supplier
     * @param exceptionClass type of exception for which to retry
     * @param maxRetries max number of retries before giving up
     * @param maxDelayBetweenRetries max delay between successive retries. The actual delay is randomly picked from
     * the interval (0, maxDelayBetweenRetries]
     * @param <V> type of supplied result
     * @return supplier
     */
    public static <V> Supplier<V> retryable(Supplier<V> base,
        Class<? extends Throwable> exceptionClass,
        int maxRetries,
        int maxDelayBetweenRetries) {
        return () -> new RetryingFunction<>(v -> base.get(),
            exceptionClass,
            maxRetries,
            maxDelayBetweenRetries).apply(null);
    }

    /**
     * Suspends the current thread for a random number of millis between 0 and
     * the indicated limit.
     * @param ms max number of millis
     */
    public static void randomDelay(int ms) {
        try {
            Thread.sleep(random.nextInt(ms));
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted", e);
        }
    }

    /**
     * Suspends the current thread for a specified number of millis and nanos.
     * @param ms number of millis
     * @param nanos number of nanos
     */
    public static void delay(int ms, int nanos) {
        try {
            Thread.sleep(ms, nanos);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted", e);
        }
    }

}
