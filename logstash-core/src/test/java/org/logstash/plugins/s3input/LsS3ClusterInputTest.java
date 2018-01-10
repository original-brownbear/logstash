package org.logstash.plugins.s3input;

import com.amazonaws.http.IdleConnectionReaper;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import org.hamcrest.MatcherAssert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.LsClusterIntegTestCase;
import org.logstash.cluster.ClusterInput;
import org.logstash.cluster.LogstashClusterConfig;
import org.logstash.cluster.elasticsearch.EsClient;
import org.logstash.ext.EventQueue;
import org.logstash.ext.JrubyEventExtLibrary;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link LsS3ClusterInput}.
 */
public final class LsS3ClusterInputTest extends LsClusterIntegTestCase {

    private static final String TEST_BUCKET = System.getProperty("org.logstash.s3it.bucket");

    private static final String TEST_REGION = System.getProperty("org.logstash.s3it.region");

    private static final String TEST_KEY = System.getProperty("org.logstash.s3it.key");

    private static final String TEST_SECRET = System.getProperty("org.logstash.s3it.secret");

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void readsOnMultipleNodes() throws Exception {
        Assume.assumeNotNull(TEST_BUCKET, TEST_REGION, TEST_KEY, TEST_SECRET);
        final CountDownLatch startJob = new CountDownLatch(1);
        final ExecutorService exec = Executors.newFixedThreadPool(3);
        try {
            final String index = "readsonmultiplenodes";
            exec.submit(() -> {
                try (EsClient configProvider =
                         EsClient.create(
                             client(), new LogstashClusterConfig(index)
                         )
                ) {
                    final Map<String, String> jobSettings = new HashMap<>();
                    jobSettings.put(
                        ClusterInput.LOGSTASH_TASK_CLASS_SETTING, LsS3ClusterInput.class.getName()
                    );
                    jobSettings.put(LsS3ClusterInput.S3_KEY_INDEX, TEST_KEY);
                    jobSettings.put(LsS3ClusterInput.S3_SECRET_INDEX, TEST_SECRET);
                    jobSettings.put(LsS3ClusterInput.S3_REGION_INDEX, TEST_REGION);
                    jobSettings.put(LsS3ClusterInput.S3_BUCKET_INDEX, TEST_BUCKET);
                    startJob.await();
                    configProvider.publishJobSettings(jobSettings);
                } catch (final InterruptedException | ExecutionException ex) {
                    throw new IllegalStateException(ex);
                }
            });
            final BlockingQueue<JrubyEventExtLibrary.RubyEvent> queue1 = new LinkedTransferQueue<>();
            final BlockingQueue<JrubyEventExtLibrary.RubyEvent> queue2 = new LinkedTransferQueue<>();
            try (EsClient configProvider1 =
                     EsClient.create(
                         client(), new LogstashClusterConfig("node1", index)
                     );
                 ClusterInput input1 = new ClusterInput(EventQueue.wrap(queue1), configProvider1);
                 EsClient configProvider2 =
                     EsClient.create(
                         client(), new LogstashClusterConfig("node2", index)
                     );
                 ClusterInput input2 = new ClusterInput(EventQueue.wrap(queue2), configProvider2)
            ) {
                exec.execute(input1);
                exec.execute(input2);
                startJob.countDown();
                MatcherAssert.assertThat(
                    queue1.take(), instanceOf(JrubyEventExtLibrary.RubyEvent.class)
                );
                MatcherAssert.assertThat(
                    queue2.take(), instanceOf(JrubyEventExtLibrary.RubyEvent.class)
                );
            }
        } finally {
            exec.shutdownNow();
            IdleConnectionReaper.shutdown();
        }
    }

    @Test
    public void readsOnOneNode() throws Exception {
        Assume.assumeNotNull(TEST_BUCKET, TEST_REGION, TEST_KEY, TEST_SECRET);
        final String index = "readsononenode";
        final LogstashClusterConfig config = new LogstashClusterConfig("node1", index);
        try (EsClient configProvider =
                 EsClient.create(client(), config)) {
            final Map<String, String> jobSettings = new HashMap<>();
            jobSettings.put(
                ClusterInput.LOGSTASH_TASK_CLASS_SETTING, LsS3ClusterInput.class.getName()
            );
            jobSettings.put(LsS3ClusterInput.S3_KEY_INDEX, TEST_KEY);
            jobSettings.put(LsS3ClusterInput.S3_SECRET_INDEX, TEST_SECRET);
            jobSettings.put(LsS3ClusterInput.S3_REGION_INDEX, TEST_REGION);
            jobSettings.put(LsS3ClusterInput.S3_BUCKET_INDEX, TEST_BUCKET);
            configProvider.publishJobSettings(jobSettings);
            final BlockingQueue<JrubyEventExtLibrary.RubyEvent> queue = new LinkedTransferQueue<>();
            final ExecutorService exec = Executors.newSingleThreadExecutor();
            try (ClusterInput input = new ClusterInput(EventQueue.wrap(queue), configProvider)) {
                exec.execute(input);
                MatcherAssert.assertThat(
                    queue.take(), instanceOf(JrubyEventExtLibrary.RubyEvent.class)
                );
            } finally {
                exec.shutdownNow();
            }
        }
        IdleConnectionReaper.shutdown();
    }
}
