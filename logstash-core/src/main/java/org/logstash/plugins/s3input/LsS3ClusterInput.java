package org.logstash.plugins.s3input;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.ClusterInput;
import org.logstash.cluster.EnqueueEvent;
import org.logstash.cluster.primitives.leadership.LeaderElector;
import org.logstash.cluster.primitives.queue.WorkQueue;
import org.logstash.ext.EventQueue;

public final class LsS3ClusterInput implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(LsS3ClusterInput.class);

    private final ClusterInput cluster;

    public LsS3ClusterInput(final ClusterInput cluster) {
        this.cluster = cluster;
    }

    @Override
    public void run() {
        try (final LeaderElector<String> elector = cluster.getCluster()
            .<String>leaderElectorBuilder().withName(ClusterInput.LEADERSHIP_IDENTIFIER).build()
        ) {
            final String localId =
                cluster.getCluster().getClusterService().getLocalNode().id().id();
            elector.run(localId);
            LOGGER.info("Indexing Bucket on Node {}", localId);
            WorkQueue<EnqueueEvent> tasks = cluster.getTasks().asWorkQueue();
        } catch (final Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static final class S3Task implements EnqueueEvent {

        private final String key;

        private final String secret;

        private final String bucket;

        private final String object;

        private S3Task(final String key, final String secret, final String bucket,
            final String object) {
            this.key = key;
            this.secret = secret;
            this.bucket = bucket;
            this.object = object;
        }

        @Override
        public void enqueue(final EventQueue queue) {

        }
    }
}
