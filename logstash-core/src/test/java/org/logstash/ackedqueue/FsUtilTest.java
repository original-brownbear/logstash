package org.logstash.ackedqueue;

import java.nio.file.Paths;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

/**
 * Tests for {@link FsUtil}.
 */
public final class FsUtilTest {

    /**
     * {@link FsUtil#hasFreeSpace(String, long)} should return true when asked for 1kb of free
     * space in a subfolder of the system's TEMP location.
     */
    @Test
    public void trueIfEnoughSpace() throws Exception {
        MatcherAssert.assertThat(
            FsUtil.hasFreeSpace(
                Paths.get(System.getProperty("java.io.tmpdir"), "some", "path").toString(),
                1024L
            ), CoreMatchers.is(true)
        );
    }

    /**
     * {@link FsUtil#hasFreeSpace(String, long)} should return false when asked for
     * {@link Long#MAX_VALUE} of free space in a subfolder of the system's TEMP location.
     */
    @Test
    public void falseIfNotEnoughSpace() throws Exception {
        MatcherAssert.assertThat(
            FsUtil.hasFreeSpace(
                Paths.get(System.getProperty("java.io.tmpdir"), "some", "path").toString(),
                Long.MAX_VALUE
            ), CoreMatchers.is(false)
        );
    }
}
