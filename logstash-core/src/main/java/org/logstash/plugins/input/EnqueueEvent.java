package org.logstash.plugins.input;

import java.io.Serializable;
import org.logstash.ext.JavaQueue;

@FunctionalInterface
public interface EnqueueEvent extends Serializable {

    void enqueue(JavaQueue queue);
}
