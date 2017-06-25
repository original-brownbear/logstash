package org.logstash.benchmark.fs;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.infra.Blackhole;

public class FSyncGCPressure extends AbstractFSyncBenchmark {

    @Benchmark
    @Group(GROUP)
    @GroupThreads(4)
    public final void actionBenchmark(final Blackhole blackhole) throws Exception {
        dumpMillionEvents(blackhole);
    }
}
