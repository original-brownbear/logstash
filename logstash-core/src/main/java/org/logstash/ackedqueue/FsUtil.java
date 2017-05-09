package org.logstash.ackedqueue;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.logstash.ackedqueue.io.AbstractByteBufferPageIO;

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

    public static long getPersistedSize(final String path) throws IOException {
        final File file = new File(path).getCanonicalFile();
        long size = 0L;
        if (file.isDirectory()) {
            for (
                final Path sub : Files.newDirectoryStream(
                file.toPath(),
                entry -> entry.getFileName().toString().matches("page\\.\\d+")
            )
                ) {
                size += getPersistedSize(sub.toString());
            }
        } else {
            try (final DataInputStream datain =
                     new DataInputStream(Files.newInputStream(file.toPath()))) {
                if (datain.read() == AbstractByteBufferPageIO.VERSION_ONE) {
                    size += 1L;
                } else {
                    throw new IllegalStateException("Broken Queue File");
                }
                while (true) {
                    datain.readLong();
                    final int len = datain.readInt();
                    if (len == 0) {
                        break;
                    }
                    final byte[] data = new byte[len];
                    datain.readFully(data);
                    final int checksum = datain.readInt();
                    final Checksum crc = new CRC32();
                    crc.update(data, 0, len);
                    if ((int) crc.getValue() != checksum) {
                        throw new EOFException();
                    }
                    size += len + 12 + 4;
                }
            } catch (final EOFException ex) {
                //ignored
            }
        }
        return size;
    }
}
