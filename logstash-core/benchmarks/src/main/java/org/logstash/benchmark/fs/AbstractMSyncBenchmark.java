package org.logstash.benchmark.fs;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

public abstract class AbstractMSyncBenchmark extends AbstractSyncBenchmark {

    private RandomAccessFile file;

    private MappedByteBuffer map;

    @Setup
    public void up() throws Exception {
        create();
        fillRandom(data);
    }

    @TearDown
    public void down() throws Exception {
        file.close();
        clean();
    }

    @Benchmark
    @Group(GROUP)
    @GroupThreads
    public void msync() throws Exception {
        file = new RandomAccessFile(tmp, "rw");
        map = file.getChannel().map(
            FileChannel.MapMode.READ_WRITE, 0L,
            (long) (MESSAGE_SIZE * MESSAGES_PER_INVOCATION)
        );
        for (int i = 0; i < MESSAGES_PER_INVOCATION; ++i) {
            data.position(0);
            map.put(data);
            if (i % SYNC_INTERVAL == 0) {
                map.force();
            }
        }
        file.close();
        tmp.delete();
    }
}
