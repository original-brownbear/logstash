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
package org.logstash.cluster.primitives.lock.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitiveTest;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.service.RaftService;
import org.logstash.cluster.time.Version;

import static org.junit.Assert.assertFalse;

/**
 * Raft lock test.
 */
public class RaftDistributedLockTest extends AbstractRaftPrimitiveTest<RaftDistributedLock> {
    @Override
    protected RaftService createService() {
        return new RaftDistributedLockService();
    }

    @Override
    protected RaftDistributedLock createPrimitive(RaftProxy proxy) {
        return new RaftDistributedLock(proxy);
    }

    /**
     * Tests locking and unlocking a lock.
     */
    @Test
    public void testLockUnlock() {
        RaftDistributedLock lock = newPrimitive("test-lock-unlock");
        lock.lock().join();
        lock.unlock().join();
    }

    /**
     * Tests releasing a lock when the client's session is closed.
     */
    @Test
    public void testReleaseOnClose() {
        RaftDistributedLock lock1 = newPrimitive("test-lock-on-close");
        RaftDistributedLock lock2 = newPrimitive("test-lock-on-close");
        lock1.lock().join();
        CompletableFuture<Version> future = lock2.lock();
        lock1.close();
        future.join();
    }

    /**
     * Tests attempting to acquire a lock with a timeout.
     */
    @Test
    public void testTryLockFail() {
        RaftDistributedLock lock1 = newPrimitive("test-try-lock-fail");
        RaftDistributedLock lock2 = newPrimitive("test-try-lock-fail");

        lock1.lock().join();

        assertFalse(lock2.tryLock(Duration.ofSeconds(1)).join().isPresent());
    }

    /**
     * Tests unlocking a lock with a blocking call in the event thread.
     */
    @Test
    public void testBlockingUnlock() {
        RaftDistributedLock lock1 = newPrimitive("test-blocking-unlock");
        RaftDistributedLock lock2 = newPrimitive("test-blocking-unlock");

        lock1.lock().thenRun(() -> {
            lock1.unlock().join();
        }).join();

        lock2.lock().join();
    }
}
