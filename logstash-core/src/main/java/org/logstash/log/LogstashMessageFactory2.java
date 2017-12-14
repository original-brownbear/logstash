package org.logstash.log;

import org.apache.logging.log4j.spi.MessageFactory2Adapter;

public final class LogstashMessageFactory2 extends MessageFactory2Adapter {

    public static final LogstashMessageFactory2 INSTANCE =
        new LogstashMessageFactory2();

    public LogstashMessageFactory2() {
        super(new LogstashMessageFactory());
    }
}
