package org.logstash;

import java.io.IOException;
import org.jruby.Ruby;
import org.logstash.ackedqueue.ext.JrubyAckedBatchExtLibrary;
import org.logstash.ackedqueue.ext.JrubyAckedQueueExtLibrary;
import org.logstash.ackedqueue.ext.JrubyAckedQueueMemoryExtLibrary;
import org.logstash.ext.JrubyEventExtLibrary;
import org.logstash.ext.JrubyTimestampExtLibrary;

/**
 * Utilities around interaction with the {@link Ruby} runtime.
 */
public final class RubyUtil {

    /**
     * Name of the Logstash JRuby module we register.
     */
    public static final String LS_MODULE_NAME = "LogStash";

    /**
     * Reference to the global {@link Ruby} runtime.
     */
    public static final Ruby RUBY = setupRuby();

    private RubyUtil() {
    }

    /**
     * Sets up the global {@link Ruby} runtime and ensures the creation of the "LogStash" module
     * on it.
     * @return Global {@link Ruby} Runtime
     */
    private static Ruby setupRuby() {
        final Ruby ruby = Ruby.getGlobalRuntime();
        ruby.getOrCreateModule(LS_MODULE_NAME);
        try {
            new JrubyAckedQueueExtLibrary().load(ruby, false);
            new JrubyAckedQueueMemoryExtLibrary().load(ruby, false);
            new JrubyTimestampExtLibrary().load(ruby, false);
            new JrubyAckedBatchExtLibrary().load(ruby, false);
            new JrubyEventExtLibrary().load(ruby, false);
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
        return ruby;
    }
}
