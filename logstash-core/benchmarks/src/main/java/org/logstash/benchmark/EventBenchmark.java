package org.logstash.benchmark;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import org.logstash.Event;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.util.NullOutputStream;

@Warmup(iterations = 3, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 10)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class EventBenchmark {

    private static final Event EVENT = new Event();

    private static final OutputStream SINK = new NullOutputStream(); 

    @Setup
    public void setUp() throws IOException {
        EVENT.setField("Foo", "Bar");
        EVENT.setField("Foo1", "Bar1");
        EVENT.setField("Foo2", "Bar2");
        EVENT.setField("Foo3", "Bar3");
        EVENT.setField("Foo4", "Bar4");
    }

    @Benchmark
    public final void pushToPersistedQueue() throws Exception {
        SINK.write(EVENT.serialize());
    }

    public static void main(final String... args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(EventBenchmark.class.getSimpleName())
            .forks(2)
            .build();
        new Runner(opt).run();
    }
}
