package org.logstash.plugins.s3input;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.ClusterInput;
import org.logstash.cluster.EnqueueEvent;
import org.logstash.cluster.LogstashClusterServer;
import org.logstash.cluster.primitives.leadership.LeaderElector;
import org.logstash.cluster.primitives.map.ConsistentTreeMap;
import org.logstash.cluster.primitives.queue.WorkQueue;
import org.logstash.ext.EventQueue;

public final class LsS3ClusterInput implements Runnable {

    public static final String S3_KEY_INDEX = "s3key";

    public static final String S3_BUCKET_INDEX = "s3bucket";

    public static final String S3_SECRET_INDEX = "s3secret";

    public static final String S3_REGION_INDEX = "s3region";

    private static final Logger LOGGER = LogManager.getLogger(LsS3ClusterInput.class);

    private static final String FINISHED_OBJECTS_MAP = "s3finishedobjects";

    private final ClusterInput cluster;

    public LsS3ClusterInput(final ClusterInput cluster) {
        this.cluster = cluster;
    }

    @Override
    public void run() {
        final LogstashClusterServer clusterServer = cluster.getCluster();
        try (final LeaderElector<String> elector = clusterServer
            .<String>leaderElectorBuilder().withName(ClusterInput.LEADERSHIP_IDENTIFIER).build();
             final ConsistentTreeMap<String, String> finishedMap = clusterServer
                 .<String, String>consistentTreeMapBuilder()
                 .withName(FINISHED_OBJECTS_MAP).build()
        ) {
            final String localId = clusterServer.getClusterService().getLocalNode().id().id();
            elector.run(localId);
            LOGGER.info("Indexing Bucket on Node {}", localId);
            final Map<String, String> config = cluster.getConfig();
            final LsS3ClusterInput.S3Config s3cfg = new LsS3ClusterInput.S3Config(
                config.get(S3_KEY_INDEX), config.get(S3_SECRET_INDEX), config.get(S3_REGION_INDEX),
                config.get(S3_BUCKET_INDEX)
            );
            try (final WorkQueue<EnqueueEvent> tasks = cluster.getTasks().asWorkQueue()) {
                for (final String object : new LsS3ClusterInput.S3PathIterator(s3cfg, finishedMap)) {
                    tasks.addOne(new LsS3ClusterInput.S3Task(s3cfg, object));
                }
            }
        } catch (final Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static final class S3Config implements Serializable {

        public final String key;
        public final String secret;
        public final String region;
        public final String bucket;

        S3Config(final String key, final String secret, final String region, final String bucket) {
            this.key = key;
            this.secret = secret;
            this.region = region;
            this.bucket = bucket;
        }
    }

    private static final class S3PathIterator implements Iterable<String> {

        private final LsS3ClusterInput.S3Config config;

        private final ConsistentTreeMap<String, String> finished;

        private S3PathIterator(final LsS3ClusterInput.S3Config config,
            final ConsistentTreeMap<String, String> finished) {
            this.config = config;
            this.finished = finished;
        }

        @Override
        public Iterator<String> iterator() {
            final AmazonS3 s3Client = AmazonS3Client.builder().withRegion(config.region)
                .withCredentials(
                    new LsS3ClusterInput.SimpleCredentialProvider(config.key, config.secret)
                ).build();
            return s3Client.listObjects(config.bucket).getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey).filter(objectKey -> !finished.containsKey(objectKey))
                .iterator();
        }
    }

    private static final class SimpleCredentialProvider implements AWSCredentialsProvider {

        private final AWSCredentials credentials;

        private SimpleCredentialProvider(final String key, final String secret) {
            this.credentials = new BasicAWSCredentials(key, secret);
        }

        @Override
        public AWSCredentials getCredentials() {
            return credentials;
        }

        @Override
        public void refresh() {
        }
    }

    private static final class S3Task implements EnqueueEvent {

        private final S3Config config;

        private final String object;

        private S3Task(final LsS3ClusterInput.S3Config config, final String object) {
            this.config = config;
            this.object = object;
        }

        @Override
        public void enqueue(final EventQueue queue) {

        }
    }
}
