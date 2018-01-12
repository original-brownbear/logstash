package org.logstash.cluster.execution;

import java.util.concurrent.TimeUnit;

public final class TimingConstants {

    public static final long TERM_LENGTH = TimeUnit.SECONDS.toMillis(30L);

    public static final long ELECTION_PERIOD = TERM_LENGTH / 10L;

    public static final long HEARTBEAT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5L);

    public static final long PARTITION_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(15L);

    private TimingConstants() {
        // Holder for Constants
    }
}
