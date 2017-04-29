package org.logstash.ackedqueue;

import java.io.File;
import java.io.IOException;

/**
 * File System Utility Methods.
 */
public final class FsUtil {

    private FsUtil() {
    }

    /**
     * Checks if the request number of bytes of free disk space are available under the given
     * path.
     * @param path Directory to check
     * @param size Bytes of free space requested
     * @return True iff the
     * @throws IOException on failure to determine free space for given path's partition
     */
    public static boolean hasFreeSpace(final String path, final long size) throws IOException {
        final File[] partitions = File.listRoots();
        File location = new File(path).getCanonicalFile();
        boolean found = false;
        while (!found) {
            if (location == null) {
                throw new IOException(
                    String.format("Unable to determine the partition that contains '%s'.", path)
                );
            }
            for (final File partition : partitions) {
                if (partition.equals(location)) {
                    found = true;
                } else {
                    location = location.getParentFile();
                }
            }
        }
        return location.getFreeSpace() >= size;
    }
}
