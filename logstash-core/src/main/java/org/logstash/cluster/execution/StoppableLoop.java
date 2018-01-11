package org.logstash.cluster.execution;

public interface StoppableLoop extends Runnable {

    void stop();

    void awaitStop();
}
