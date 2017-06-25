package org.logstash.benchmark.fs;

import com.google.common.io.Files;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.output.NullOutputStream;
import org.logstash.Event;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 30, time = 1)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public abstract class AbstractSyncBenchmark {

    public static final String GROUP = "GROUP";

    protected static final int SYNC_INTERVAL = 1000;

    protected static final int MESSAGES_PER_INVOCATION = 500_000;

    protected static final int MESSAGE_SIZE = 200;

    protected final ByteBuffer data = ByteBuffer.allocateDirect(200);

    protected File tmp;

    protected final void create() {
        tmp = new File(Files.createTempDir().toPath().resolve("tmp.tmp").toString());
    }

    protected final void clean() {
        tmp.delete();
    }

    protected static void dumpMillionEvents(final Blackhole blackhole) throws Exception {
        for (int i = 0; i < 1_000_000; ++i) {
            final Event event = new Event();
            event.setField("foo", "bar");
            final byte[] raw = event.serialize();
            NullOutputStream.NULL_OUTPUT_STREAM.write(raw);
            blackhole.consume(event);
            final Event deserialized = Event.deserialize(raw);
            blackhole.consume(raw);
            blackhole.consume(deserialized);
        }
    }

    /**
     * Prevent potential compression of written data.
     * @param buffer Buffer to fill with random data
     */
    protected static void fillRandom(final ByteBuffer buffer) {
        final byte[] random = new byte[buffer.capacity()];
        ThreadLocalRandom.current().nextBytes(random);
        buffer.clear();
        buffer.put(random).flip();
    }
}
