/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.logstash.cluster.primitives.queue.impl;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitive;
import org.logstash.cluster.primitives.queue.AsyncWorkQueue;
import org.logstash.cluster.primitives.queue.Task;
import org.logstash.cluster.primitives.queue.WorkQueue;
import org.logstash.cluster.primitives.queue.WorkQueueStats;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.utils.AbstractAccumulator;
import org.logstash.cluster.utils.Accumulator;
import org.logstash.cluster.utils.concurrent.Threads;
import org.slf4j.Logger;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Distributed resource providing the {@link WorkQueue} primitive.
 */
public class RaftWorkQueue extends AbstractRaftPrimitive implements AsyncWorkQueue<byte[]> {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftWorkQueueOperations.NAMESPACE)
        .register(RaftWorkQueueEvents.NAMESPACE)
        .build());

    private final Logger log = getLogger(getClass());
    private final ExecutorService executor;
    private final AtomicReference<TaskProcessor> taskProcessor = new AtomicReference<>();
    private final Timer timer = new Timer("atomix-work-queue-completer");
    private final AtomicBoolean isRegistered = new AtomicBoolean(false);

    public RaftWorkQueue(RaftProxy proxy) {
        super(proxy);
        executor = newSingleThreadExecutor(Threads.namedThreads("atomix-work-queue-" + proxy.name() + "-%d", log));
        proxy.addStateChangeListener(state -> {
            if (state == RaftProxy.State.CONNECTED && isRegistered.get()) {
                proxy.invoke(RaftWorkQueueOperations.REGISTER);
            }
        });
        proxy.addEventListener(RaftWorkQueueEvents.TASK_AVAILABLE, this::resumeWork);
    }

    @Override
    public CompletableFuture<Void> destroy() {
        executor.shutdown();
        timer.cancel();
        return proxy.invoke(RaftWorkQueueOperations.CLEAR);
    }

    @Override
    public CompletableFuture<Void> registerTaskProcessor(Consumer<byte[]> callback,
        int parallelism,
        Executor executor) {
        Accumulator<String> completedTaskAccumulator =
            new CompletedTaskAccumulator(timer, 50, 50); // TODO: make configurable
        taskProcessor.set(new TaskProcessor(callback,
            parallelism,
            executor,
            completedTaskAccumulator));
        return register().thenCompose(v -> take(parallelism))
            .thenAccept(taskProcessor.get());
    }

    @Override
    public CompletableFuture<Void> stopProcessing() {
        return unregister();
    }

    @Override
    public CompletableFuture<WorkQueueStats> stats() {
        return proxy.invoke(RaftWorkQueueOperations.STATS, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Void> complete(Collection<String> taskIds) {
        if (taskIds.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return proxy.invoke(RaftWorkQueueOperations.COMPLETE, SERIALIZER::encode, new RaftWorkQueueOperations.Complete(taskIds));
    }

    @Override
    public CompletableFuture<Void> addMultiple(Collection<byte[]> items) {
        if (items.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return proxy.invoke(RaftWorkQueueOperations.ADD, SERIALIZER::encode, new RaftWorkQueueOperations.Add(items));
    }

    @Override
    public CompletableFuture<Collection<Task<byte[]>>> take(int maxTasks) {
        if (maxTasks <= 0) {
            return CompletableFuture.completedFuture(ImmutableList.of());
        }
        return proxy.invoke(RaftWorkQueueOperations.TAKE, SERIALIZER::encode, new RaftWorkQueueOperations.Take(maxTasks), SERIALIZER::decode);
    }

    private CompletableFuture<Void> unregister() {
        return proxy.invoke(RaftWorkQueueOperations.UNREGISTER).thenRun(() -> isRegistered.set(false));
    }

    private CompletableFuture<Void> register() {
        return proxy.invoke(RaftWorkQueueOperations.REGISTER).thenRun(() -> isRegistered.set(true));
    }

    private void resumeWork() {
        TaskProcessor activeProcessor = taskProcessor.get();
        if (activeProcessor == null) {
            return;
        }
        this.take(activeProcessor.headRoom())
            .whenCompleteAsync((tasks, e) -> activeProcessor.accept(tasks), executor);
    }

    // TaskId accumulator for paced triggering of task completion calls.
    private class CompletedTaskAccumulator extends AbstractAccumulator<String> {
        CompletedTaskAccumulator(Timer timer, int maxTasksToBatch, int maxBatchMillis) {
            super(timer, maxTasksToBatch, maxBatchMillis, Integer.MAX_VALUE);
        }

        @Override
        public void processItems(List<String> items) {
            complete(items);
        }
    }

    private class TaskProcessor implements Consumer<Collection<Task<byte[]>>> {

        private final AtomicInteger headRoom;
        private final Consumer<byte[]> backingConsumer;
        private final Executor executor;
        private final Accumulator<String> taskCompleter;

        public TaskProcessor(Consumer<byte[]> backingConsumer,
            int parallelism,
            Executor executor,
            Accumulator<String> taskCompleter) {
            this.backingConsumer = backingConsumer;
            this.headRoom = new AtomicInteger(parallelism);
            this.executor = executor;
            this.taskCompleter = taskCompleter;
        }

        public int headRoom() {
            return headRoom.get();
        }

        @Override
        public void accept(Collection<Task<byte[]>> tasks) {
            if (tasks == null) {
                return;
            }
            headRoom.addAndGet(-1 * tasks.size());
            tasks.forEach(task ->
                executor.execute(() -> {
                    try {
                        backingConsumer.accept(task.payload());
                        taskCompleter.add(task.taskId());
                    } catch (Exception e) {
                        log.debug("Task execution failed", e);
                    } finally {
                        headRoom.incrementAndGet();
                        resumeWork();
                    }
                }));
        }
    }
}
