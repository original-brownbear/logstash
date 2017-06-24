package org.logstash.benchmark;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 3, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class MSyncBenchmark {

    private AtomicLong counter;

    private File tmp;

    private RandomAccessFile file;

    private MappedByteBuffer map;

    private ByteBuffer data = ByteBuffer.allocateDirect(200);

    @Setup
    public void up() throws Exception {
        counter = new AtomicLong(0);
        tmp = new File("tmp.tmp");
        file = new RandomAccessFile(tmp, "rw");
        map = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 200 * 500_000);
    }

    @TearDown
    public void down() throws Exception {
        file.close();
        tmp.delete();
    }

    @Benchmark
    @Group("g")
    @GroupThreads
    public void msync() throws Exception {
        for (int i = 0; i < 500_000; ++i) {
            data.clear();
            map.put(data);
            if (i % 1000 == 0) {
                map.force();
            }
        }
        map.position(0);
    }
}
