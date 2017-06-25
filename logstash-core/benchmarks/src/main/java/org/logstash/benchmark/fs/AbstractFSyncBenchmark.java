package org.logstash.benchmark.fs;

import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

public abstract class AbstractFSyncBenchmark extends AbstractSyncBenchmark {

    private FileChannel file;

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
    public void fsync() throws Exception {
        file = FileChannel.open(
            tmp.toPath(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        );
        for (int i = 0; i < MESSAGES_PER_INVOCATION; ++i) {
            data.position(0);
            file.write(data);
            if (i % SYNC_INTERVAL == 0) {
                file.force(true);
            }
        }
        file.close();
        tmp.delete();
    }
}
