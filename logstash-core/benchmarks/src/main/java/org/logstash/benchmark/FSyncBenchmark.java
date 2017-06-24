package org.logstash.benchmark;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
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

/**
 * /Library/Java/JavaVirtualMachines/jdk1.8.0_40.jdk/Contents/Home/bin/java -Dfile.encoding=UTF-8 -classpath /Users/brownbear/src/logstash/logstash-core/benchmarks/build/classes/java/main:/Users/brownbear/src/logstash/logstash-core/benchmarks/build/resources/main:/Users/brownbear/src/logstash/logstash-core/build/classes/java/main:/Users/brownbear/src/logstash/logstash-core/build/resources/main:/Users/brownbear/.gradle/caches/modules-2/files-2.1/org.openjdk.jmh/jmh-generator-annprocess/1.18/b852fb028de645ad2852bbe998e084d253f450a5/jmh-generator-annprocess-1.18.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/org.openjdk.jmh/jmh-core-benchmarks/1.18/7d45d7d839852247a82ebff7bc0cb0a599a81b1d/jmh-core-benchmarks-1.18.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/org.openjdk.jmh/jmh-core/1.18/174aa0077e9db596e53d7f9ec37556d9392d5a6/jmh-core-1.18.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/net.sf.jopt-simple/jopt-simple/5.0.3/cdd846cfc4e0f7eefafc02c0f5dce32b9303aa2a/jopt-simple-5.0.3.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.google.guava/guava/21.0/3a3d111be1be1b745edfa7d91678a12d7ed38709/guava-21.0.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/commons-io/commons-io/2.5/2852e6e05fbb95076fc091f6d1780f1f8fe35e0f/commons-io-2.5.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/org.apache.logging.log4j/log4j-core/2.6.2/a91369f655eb1639c6aece5c5eb5108db18306/log4j-core-2.6.2.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/org.apache.logging.log4j/log4j-api/2.6.2/bd1b74a5d170686362091c7cf596bbc3adf5c09b/log4j-api-2.6.2.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/org.jruby/jruby-core/9.1.10.0/36f2bb089d8cb4cbd7dc1fb2fdbbdb270e1c4be2/jruby-core-9.1.10.0.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.module/jackson-module-afterburner/2.7.4/c907df424e3d79c87ef97d6078281f74f1c5f8/jackson-module-afterburner-2.7.4.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/joda-time/joda-time/2.8.2/d27c24204c5e507b16fec01006b3d0f1ec42aed4/joda-time-2.8.2.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-databind/2.7.4/1e9c6f3659644aeac84872c3b62d8e363bf4c96d/jackson-databind-2.7.4.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.dataformat/jackson-dataformat-cbor/2.7.4/bd379f8d9152df26b024ea3e1c00b75bcce60d57/jackson-dataformat-cbor-2.7.4.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-core/2.7.4/b8f38a249116b66d804a5ca2b14a3459b7913a94/jackson-core-2.7.4.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-math3/3.2/ec2544ab27e110d2d431bdad7d538ed509b21e62/commons-math3-3.2.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-annotations/2.7.0/19f42c154ffc689f40a77613bc32caeb17d744e3/jackson-annotations-2.7.0.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.github.jnr/jnr-netdb/1.1.6/ac1e5af7edfda95086679f27a35b18f10f4c88b5/jnr-netdb-1.1.6.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.github.jnr/jnr-unixsocket/0.17/735e0ea011141da52b1a58b6d344c2c70c287c03/jnr-unixsocket-0.17.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.github.jnr/jnr-enxio/0.16/ae38d71e2c224e3ea0f9528ed026c4e5a9f2b851/jnr-enxio-0.16.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.github.jnr/jnr-x86asm/1.0.2/6936bbd6c5b235665d87bd450f5e13b52d4b48/jnr-x86asm-1.0.2.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.github.jnr/jnr-posix/3.0.41/36eff018149e53ed814a340ddb7de73ceb66bf96/jnr-posix-3.0.41.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.github.jnr/jnr-constants/0.9.9/33f23994e09aeb49880aa01e12e8e9eff058c14c/jnr-constants-0.9.9.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.github.jnr/jffi/1.2.15/f480f0234dd8f053da2421e60574cfbd9d85e1f5/jffi-1.2.15.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.github.jnr/jffi/1.2.15/53f344e9e60e16f648dc66ce7cb8b1e7499b2a9/jffi-1.2.15-native.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/org.jruby.joni/joni/2.1.11/655cc3aba1bc9dbdd653f28937bec16f3e9c4cec/joni-2.1.11.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/org.jruby.extras/bytelist/1.0.15/80294e59315b314d57782dc37f983ae5b29c2e4c/bytelist-1.0.15.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/org.jruby.jcodings/jcodings/1.0.18/e2c76a19f00128bb1806207e2989139bfb45f49d/jcodings-1.0.18.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/org.jruby/dirgra/0.3/fcdf20c966ff7bd3299c3d7fb3e7bfb14e38d4ee/dirgra-0.3.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.headius/invokebinder/1.7/7085e420f9f36baf0ff614050ea7a8a3bf78287f/invokebinder-1.7.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.headius/options/1.4/34bd3630af4e516fdb4d90d16732b8a3dd2f7b46/options-1.4.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.headius/unsafe-fences/1.0/9c023c37909b9ea17e49b596be1ede6d0b8dead4/unsafe-fences-1.0.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.jcraft/jzlib/1.1.3/c01428efa717624f7aabf4df319939dda9646b2d/jzlib-1.1.3.jar:/Users/brownbear/.gradle/caches/modules-2/files-2.1/com.martiansoftware/nailgun-server/0.9.1/d57ea0a6f6c1bb1b616c5b3b311b3726c6ff35ad/nailgun-server-0.9.1.jar org.openjdk.jmh.Main org.logstash.benchmark.MSyncBenchmark.*
 * # JMH 1.18 (released 103 days ago)
 * # VM version: JDK 1.8.0_40, VM 25.40-b25
 * # VM invoker: /Library/Java/JavaVirtualMachines/jdk1.8.0_40.jdk/Contents/Home/jre/bin/java
 * # VM options: -Dfile.encoding=UTF-8
 * # Warmup: 3 iterations, 100 ms each
 * # Measurement: 10 iterations, 100 ms each
 * # Timeout: 10 min per iteration
 * # Threads: 4 threads (1 group; 1x "fsync", 3x "increment" in each group), will synchronize iterations
 * # Benchmark mode: Throughput, ops/time
 * # Benchmark: org.logstash.benchmark.MSyncBenchmark.g
 *
 * # Run progress: 0.00% complete, ETA 00:00:01
 * # Fork: 1 of 1
 * # Warmup Iteration   1: 340.903 ops/s
 * # Warmup Iteration   2: 499.539 ops/s
 * # Warmup Iteration   3: 531.199 ops/s
 * Iteration   1: 517.095 ops/s
 * fsync:     0.628 ops/s
 * increment: 516.467 ops/s
 *
 * Iteration   2: 543.772 ops/s
 * fsync:     0.616 ops/s
 * increment: 543.156 ops/s
 *
 * Iteration   3: 531.509 ops/s
 * fsync:     0.613 ops/s
 * increment: 530.896 ops/s
 *
 * Iteration   4: 551.093 ops/s
 * fsync:     0.628 ops/s
 * increment: 550.465 ops/s
 *
 * Iteration   5: 553.680 ops/s
 * fsync:     0.621 ops/s
 * increment: 553.059 ops/s
 *
 * Iteration   6: 538.340 ops/s
 * fsync:     0.597 ops/s
 * increment: 537.743 ops/s
 *
 * Iteration   7:
 * Process finished with exit code 137 (interrupted by signal 9: SIGKILL)
 */
@Warmup(iterations = 3, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class FSyncBenchmark {

    private AtomicLong counter;

    private File tmp;

    private FileChannel file;

    private ByteBuffer data = ByteBuffer.allocateDirect(200);

    @Setup
    public void up() throws Exception {
        counter = new AtomicLong(0);
        tmp = new File("tmp.tmp");
        file = FileChannel
            .open(tmp.toPath(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            );
    }

    @TearDown
    public void down() throws Exception {
        file.close();
        tmp.delete();
    }

    @Benchmark
    @Group("g")
    @GroupThreads
    public void fsync() throws Exception {
        for (int i = 0; i < 500_000; ++i) {
            data.clear();
            file.write(data);
            if (i % 1000 == 0) {
                file.force(true);
            }
        }
        file.position(0);
    }
}
