package org.logstash.persistedqueue;

import java.util.concurrent.TimeUnit;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.logstash.Event;
import org.logstash.ext.JrubyEventExtLibrary;

@JRubyClass(name = "PqLocal")
public class PqLocalRuby {

    private final PersistedQueue queue;

    /**
     * Ctor.
     * @param ack Maximum number of in flight {@link Event}
     * @param directory Directory to store backing data in
     */
    public PqLocalRuby(final int ack, final String directory) {
        this.queue = new PersistedQueue.Local(ack, directory);
    }

    public void enqueue(final JrubyEventExtLibrary.RubyEvent event)
        throws InterruptedException {
        this.queue.enqueue(event.getEvent());
    }

    @JRubyMethod(name = "dequeue")
    public JrubyEventExtLibrary.RubyEvent dequeueJava(final ThreadContext context)
        throws InterruptedException {
        return JrubyEventExtLibrary.RubyEvent.newRubyEvent(context.runtime, this.queue.dequeue());
    }

    @JRubyMethod(name = "poll", required = 2)
    public JrubyEventExtLibrary.RubyEvent pollJava(final ThreadContext context,
        final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        final Event event = this.queue.poll(timeout, timeUnit);
        final JrubyEventExtLibrary.RubyEvent result;
        if (event == null) {
            result = null;
        } else {
            result = JrubyEventExtLibrary.RubyEvent.newRubyEvent(context.runtime, event);
        }
        return result;
    }
}
