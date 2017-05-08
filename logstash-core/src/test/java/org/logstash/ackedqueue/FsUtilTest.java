package org.logstash.ackedqueue;

import java.io.File;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link FsUtil}.
 */
public final class FsUtilTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    /**
     * {@link FsUtil#hasFreeSpace(String, long)} should return true when asked for 1kb of free
     * space in a subfolder of the system's TEMP location.
     */
    @Test
    public void trueIfEnoughSpace() throws Exception {
        MatcherAssert.assertThat(
            FsUtil.hasFreeSpace(temp.newFolder().getAbsolutePath(), 1024L),
            CoreMatchers.is(true)
        );
    }

    /**
     * {@link FsUtil#hasFreeSpace(String, long)} should return false when asked for
     * {@link Long#MAX_VALUE} of free space in a subfolder of the system's TEMP location.
     */
    @Test
    public void falseIfNotEnoughSpace() throws Exception {
        MatcherAssert.assertThat(
            FsUtil.hasFreeSpace(temp.newFolder().getAbsolutePath(), Long.MAX_VALUE),
            CoreMatchers.is(false)
        );
    }

    @Test
    public void getPersistedSize() throws Exception {
        final File folder = this.temp.newFolder();
        Settings settings = TestSettings.persistedQueueSettings(100, folder.getAbsolutePath());
        try (final Queue queue = new Queue(settings)) {
            queue.open();
            for (int i = 0; i < 50; ++i) {
                queue.write(new StringElement("foooo"));
            }
            queue.ensurePersistedUpto(queue.nextSeqNum());
            final long size = queue.getPersistedByteSize();
            queue.close();
            MatcherAssert.assertThat(
                FsUtil.getPersistedSize(folder.getAbsolutePath()),
                CoreMatchers.is(size)
            );
        }
    }
}
