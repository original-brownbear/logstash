package org.logstash.cluster.messaging.netty;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import java.net.ConnectException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.logstash.TestUtils;
import org.logstash.cluster.messaging.Endpoint;
import org.logstash.cluster.messaging.ManagedMessagingService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Netty messaging service test.
 */
public final class NettyMessagingServiceTest {

    private ManagedMessagingService netty1;
    private ManagedMessagingService netty2;
    private Endpoint ep1;
    private Endpoint ep2;
    private Endpoint invalidEndPoint;

    @Before
    public void setUp() {
        ep1 = new Endpoint(InetAddress.getLoopbackAddress(), TestUtils.freePort());
        netty1 = (ManagedMessagingService) NettyMessagingService.builder()
            .withEndpoint(ep1)
            .build()
            .open()
            .join();
        ep2 = new Endpoint(InetAddress.getLoopbackAddress(), TestUtils.freePort());
        netty2 = (ManagedMessagingService) NettyMessagingService.builder()
            .withEndpoint(ep2)
            .build()
            .open()
            .join();
        invalidEndPoint = new Endpoint(InetAddress.getLoopbackAddress(), TestUtils.freePort());
    }

    @After
    public void tearDown() {
        if (netty1 != null) {
            netty1.close();
        }

        if (netty2 != null) {
            netty2.close();
        }
    }

    @Test
    public void testSendAsync() {
        final String subject = nextSubject();
        final CountDownLatch latch1 = new CountDownLatch(1);
        CompletableFuture<Void> response = netty1.sendAsync(ep2, subject, "hello world".getBytes());
        response.whenComplete((r, e) -> {
            assertNull(e);
            latch1.countDown();
        });
        Uninterruptibles.awaitUninterruptibly(latch1);

        final CountDownLatch latch2 = new CountDownLatch(1);
        response = netty1.sendAsync(invalidEndPoint, subject, "hello world".getBytes());
        response.whenComplete((r, e) -> {
            assertNotNull(e);
            assertTrue(e instanceof ConnectException);
            latch2.countDown();
        });
        Uninterruptibles.awaitUninterruptibly(latch2);
    }

    /**
     * Returns a random String to be used as a test subject.
     * @return string
     */
    private String nextSubject() {
        return UUID.randomUUID().toString();
    }

    @Test
    @Ignore // FIXME disabled on 9/29/16 due to random failures
    public void testSendAndReceive() {
        final String subject = nextSubject();
        final AtomicBoolean handlerInvoked = new AtomicBoolean(false);
        final AtomicReference<byte[]> request = new AtomicReference<>();
        final AtomicReference<Endpoint> sender = new AtomicReference<>();
        final byte[] rawResponse = "hello there".getBytes(StandardCharsets.UTF_8);
        final byte[] rawRequest = "hello world".getBytes(StandardCharsets.UTF_8);
        final BiFunction<Endpoint, byte[], byte[]> handler = (ep, data) -> {
            handlerInvoked.set(true);
            sender.set(ep);
            request.set(data);
            return rawResponse;
        };
        netty2.registerHandler(subject, handler, MoreExecutors.directExecutor());
        final CompletableFuture<byte[]> response =
            netty1.sendAndReceive(ep2, subject, rawRequest);
        assertTrue(Arrays.equals(rawResponse, response.join()));
        assertTrue(handlerInvoked.get());
        assertTrue(Arrays.equals(request.get(), rawRequest));
        assertEquals(ep1, sender.get());
    }

    @Test
    public void testSendTimeout() {
        final String subject = nextSubject();
        final BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> handler = (ep, payload) -> new CompletableFuture<>();
        netty2.registerHandler(subject, handler);

        try {
            netty1.sendAndReceive(ep2, subject, "hello world".getBytes()).join();
            fail();
        } catch (final CompletionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    /**
     * Supplies executors when registering a handler and calling sendAndReceive and verifies the request handling
     * and response completion occurs on the expected thread.
     */
    @Test
    @Ignore
    public void testSendAndReceiveWithExecutor() {
        final String subject = nextSubject();
        final ExecutorService completionExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "completion-thread"));
        final ExecutorService handlerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "handler-thread"));
        final AtomicReference<String> handlerThreadName = new AtomicReference<>();
        final AtomicReference<String> completionThreadName = new AtomicReference<>();

        final CountDownLatch latch = new CountDownLatch(1);

        final BiFunction<Endpoint, byte[], byte[]> handler = (ep, data) -> {
            handlerThreadName.set(Thread.currentThread().getName());
            try {
                latch.await();
            } catch (final InterruptedException ignored) {
                Thread.currentThread().interrupt();
                fail("InterruptedException");
            }
            return "hello there".getBytes();
        };
        netty2.registerHandler(subject, handler, handlerExecutor);

        final CompletableFuture<byte[]> response = netty1.sendAndReceive(
            ep2,
            subject,
            "hello world".getBytes(),
            completionExecutor);
        response.whenComplete((r, e) -> completionThreadName.set(Thread.currentThread().getName()));
        latch.countDown();

        // Verify that the message was request handling and response completion happens on the correct thread.
        assertTrue(Arrays.equals("hello there".getBytes(), response.join()));
        assertEquals("completion-thread", completionThreadName.get());
        assertEquals("handler-thread", handlerThreadName.get());
    }
}
