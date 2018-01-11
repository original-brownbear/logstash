package org.logstash.cluster;

import java.io.Serializable;
import org.logstash.ext.EventQueue;

@FunctionalInterface
public interface WorkerTask extends Serializable {

    void enqueueEvents(ClusterInput cluster, EventQueue queue);
}
