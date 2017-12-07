package org.logstash.plugins.input;

import java.io.Serializable;
import org.logstash.ext.EventQueue;

@FunctionalInterface
public interface EnqueueEvent extends Serializable {

    void enqueue(EventQueue queue);
}
