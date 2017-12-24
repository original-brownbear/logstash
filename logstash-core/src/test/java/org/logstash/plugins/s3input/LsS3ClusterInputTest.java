package org.logstash.plugins.s3input;

import com.amazonaws.http.IdleConnectionReaper;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.TestUtils;
import org.logstash.cluster.ClusterConfigProvider;
import org.logstash.cluster.ClusterInput;
import org.logstash.cluster.LogstashClusterConfig;
import org.logstash.ext.JrubyEventExtLibrary;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link LsS3ClusterInput}.
 */
public final class LsS3ClusterInputTest extends ESIntegTestCase {

    private static final String TEST_BUCKET = System.getProperty("org.logstash.s3it.bucket");

    private static final String TEST_REGION = System.getProperty("org.logstash.s3it.region");

    private static final String TEST_KEY = System.getProperty("org.logstash.s3it.key");

    private static final String TEST_SECRET = System.getProperty("org.logstash.s3it.secret");

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void readsOnMultipleNodes() throws Exception {
        Assume.assumeNotNull(TEST_BUCKET, TEST_REGION, TEST_KEY, TEST_SECRET);
        ensureGreen();
        System.setSecurityManager(null);
        final String index = "readsonmultiplenodes";
        final LogstashClusterConfig config = new LogstashClusterConfig(
            "node1",
            new InetSocketAddress(InetAddress.getLoopbackAddress(), TestUtils.freePort()),
            Collections.emptyList(), temporaryFolder.newFolder(), index
        );
        try (ClusterConfigProvider configProvider =
                 ClusterConfigProvider.esConfigProvider(client(), config)) {
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
            try (final ClusterInput input =
                     new ClusterInput(
                         event -> {
                             try {
                                 queue.put(event);
                             } catch (final InterruptedException ex) {
                                 throw new IllegalStateException(ex);
                             }
                         }, configProvider
                     )
            ) {
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
