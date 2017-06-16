package org.logstash.persistedqueue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.RubyNumeric;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;
import org.logstash.Event;
import org.logstash.ext.JrubyEventExtLibrary;

public class JRubyPqLocalExtLibrary implements Library {

    @Override
    public void load(final Ruby runtime, final boolean wrap) throws IOException {
        final RubyModule module = runtime.defineModule("LogStash");
        final RubyClass clazz =
            runtime.defineClassUnder("PqLocal", runtime.getObject(), RubyPqLocal::new, module);
        clazz.defineAnnotatedMethods(RubyPqLocal.class);
    }

    @JRubyClass(name = "PqLocal")
    public class RubyPqLocal extends RubyObject {

        private PersistedQueue queue;

        public RubyPqLocal(final Ruby runtime, final RubyClass klass) {
            super(runtime, klass);
        }

        @JRubyMethod(name = "initialize", required = 2)
        public IRubyObject init(final ThreadContext context, final IRubyObject ack,
            final IRubyObject directory) {
            this.queue =
                new PersistedQueue.Local(RubyNumeric.fix2int(ack), directory.asJavaString());
            return context.nil;
        }

        @JRubyMethod(name = "enqueue", required = 1)
        public void enqueueJava(final ThreadContext context,
            final IRubyObject event) throws InterruptedException {
            this.queue.enqueue(((JrubyEventExtLibrary.RubyEvent) event).getEvent());
        }

        @JRubyMethod(name = "dequeue")
        public JrubyEventExtLibrary.RubyEvent dequeueJava(final ThreadContext context)
            throws InterruptedException {
            return JrubyEventExtLibrary.RubyEvent
                .newRubyEvent(context.runtime, this.queue.dequeue());
        }

        @JRubyMethod(name = "poll", required = 1)
        public IRubyObject pollJava(final ThreadContext context,
            final IRubyObject timeout) throws InterruptedException {
            final Event event = this.queue
                .poll(RubyNumeric.fix2long(timeout), TimeUnit.MILLISECONDS);
            final IRubyObject result;
            if (event == null) {
                result = context.nil;
            } else {
                result = JrubyEventExtLibrary.RubyEvent.newRubyEvent(context.runtime, event);
            }
            return result;
        }
    }
}
