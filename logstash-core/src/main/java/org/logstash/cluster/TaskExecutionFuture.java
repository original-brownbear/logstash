package org.logstash.cluster;

import java.util.concurrent.Executor;

class TaskExecutionFuture {

    private final TaskId taskId;

    private final Executor executor;

    TaskExecutionFuture(final TaskId taskId, final Executor executor) {
        this.taskId = taskId;
        this.executor = executor;
    }

    public boolean awaitFinish() {
        return true;
    }

    public void onFailure(Runnable handler) {

    }
}
