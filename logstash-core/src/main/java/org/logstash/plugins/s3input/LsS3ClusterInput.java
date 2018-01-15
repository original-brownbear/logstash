package org.logstash.plugins.s3input;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.Event;
import org.logstash.FieldReference;
import org.logstash.RubyUtil;
import org.logstash.cluster.ClusterInput;
import org.logstash.cluster.WorkerTask;
import org.logstash.cluster.elasticsearch.EsClient;
import org.logstash.cluster.elasticsearch.primitives.EsMap;
import org.logstash.cluster.state.TaskQueue;
import org.logstash.ext.EventQueue;
import org.logstash.ext.JrubyEventExtLibrary;

public final class LsS3ClusterInput implements Runnable {

    public static final String PROCESSING_STATE = "PROCESSING";

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
        final EsClient esClient = cluster.getEsClient();
        final String localId = esClient.getConfig().localNode();
        try {
            LOGGER.info("Indexing Bucket on Node {}", localId);
            final Map<String, Object> config = cluster.getConfig();
            final LsS3ClusterInput.S3Config s3cfg = new LsS3ClusterInput.S3Config(
                (String) config.get(S3_KEY_INDEX), (String) config.get(S3_SECRET_INDEX),
                (String) config.get(S3_REGION_INDEX),
                (String) config.get(S3_BUCKET_INDEX)
            );
            final TaskQueue tasks = cluster.getTasks();
            final EsMap finishedMap = esClient.map(FINISHED_OBJECTS_MAP);
            int found = 0;
            for (final String object : new LsS3ClusterInput.S3PathIterator(s3cfg, finishedMap)) {
                found++;
                LOGGER.info("Enqueuing task to process {}", object);
                tasks.pushTask(new LsS3ClusterInput.S3Task(s3cfg, object));
                finishedMap.put(object, PROCESSING_STATE);
            }
            LOGGER.info(
                "Found {} objects to download.\nListening for new objects in S3.",
                found
            );
        } catch (final Exception ex) {
            LOGGER.error("Exception in S3 Input Main Loop:", ex);
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

        private final EsMap finished;

        private S3PathIterator(final LsS3ClusterInput.S3Config config, final EsMap finished) {
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

    private static final class S3Task implements WorkerTask {

        private static final Logger LOGGER = LogManager.getLogger(LsS3ClusterInput.S3Task.class);

        private final S3Config config;

        private final String object;

        private S3Task(final LsS3ClusterInput.S3Config config, final String object) {
            this.config = config;
            this.object = object;
        }

        @Override
        public void enqueueEvents(final ClusterInput cluster, final EventQueue queue) {
            final String localNode = cluster.getEsClient().getConfig().localNode();
            LOGGER.info("Processing {} on {}", object, localNode);
            final AmazonS3 s3Client = AmazonS3Client.builder().withRegion(config.region)
                .withCredentials(
                    new LsS3ClusterInput.SimpleCredentialProvider(config.key, config.secret)
                ).build();
            try (final S3Object obj = s3Client.getObject(config.bucket, object)) {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                IOUtils.copy(obj.getObjectContent(), baos);
                for (final String line : baos.toString("UTF-8").split("\n")) {
                    final Event event = new Event();
                    event.setField(FieldReference.from("message"), line);
                    queue.push(JrubyEventExtLibrary.RubyEvent.newRubyEvent(RubyUtil.RUBY, event));
                }
            } catch (final IOException ex) {
                throw new IllegalStateException(ex);
            }
            LOGGER.info("Successfully processed {} on {}", object, localNode);
        }
    }
}
