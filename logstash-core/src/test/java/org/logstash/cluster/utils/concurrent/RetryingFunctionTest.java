package org.logstash.cluster.utils.concurrent;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Retrying function test.
 */
public class RetryingFunctionTest {
    private int round;

    @Before
    public void setUp() {
        round = 1;
    }

    @After
    public void tearDown() {
        round = 0;
    }

    @Test(expected = RetryableException.class)
    public void testNoRetries() {
        new RetryingFunction<>(this::succeedAfterOneFailure, RetryableException.class, 0, 10).apply(null);
    }

    @Test
    public void testSuccessAfterOneRetry() {
        new RetryingFunction<>(this::succeedAfterOneFailure, RetryableException.class, 1, 10).apply(null);
    }

    @Test(expected = RetryableException.class)
    public void testFailureAfterOneRetry() {
        new RetryingFunction<>(this::succeedAfterTwoFailures, RetryableException.class, 1, 10).apply(null);
    }

    @Test
    public void testFailureAfterTwoRetries() {
        new RetryingFunction<>(this::succeedAfterTwoFailures, RetryableException.class, 2, 10).apply(null);
    }

    @Test(expected = NonRetryableException.class)
    public void testFailureWithNonRetryableFailure() {
        new RetryingFunction<>(this::failCompletely, RetryableException.class, 2, 10).apply(null);
    }

    private String succeedAfterOneFailure(String input) {
        if (round++ <= 1) {
            throw new RetryableException();
        } else {
            return "pass";
        }
    }

    private String succeedAfterTwoFailures(String input) {
        if (round++ <= 2) {
            throw new RetryableException();
        } else {
            return "pass";
        }
    }

    private String failCompletely(String input) {
        if (round++ <= 1) {
            throw new NonRetryableException();
        } else {
            return "pass";
        }
    }

    private class RetryableException extends RuntimeException {
    }

    private class NonRetryableException extends RuntimeException {
    }
}
