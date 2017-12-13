package org.logstash.cluster;

import java.io.Serializable;
import org.logstash.ext.EventQueue;

@FunctionalInterface
public interface EnqueueEvent extends Serializable {

    void enqueue(EventQueue queue);
}
