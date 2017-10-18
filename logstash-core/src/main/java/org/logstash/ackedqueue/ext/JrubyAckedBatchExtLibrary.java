package org.logstash.ackedqueue.ext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;
import org.logstash.Event;
import org.logstash.RubyUtil;
import org.logstash.ackedqueue.Batch;
import org.logstash.ackedqueue.Queueable;
import org.logstash.ackedqueue.io.LongVector;
import org.logstash.ext.JrubyEventExtLibrary;

public final class JrubyAckedBatchExtLibrary implements Library {

    @Override
    public void load(Ruby runtime, boolean wrap) {
        RubyModule module = RubyUtil.LOGSTASH_MODULE;

        RubyClass clazz = runtime.defineClassUnder("AckedBatch", runtime.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby runtime, RubyClass rubyClass) {
                return new RubyAckedBatch(runtime, rubyClass);
            }
        }, module);

        clazz.defineAnnotatedMethods(RubyAckedBatch.class);
    }

    @JRubyClass(name = "AckedBatch")
    public static final class RubyAckedBatch extends RubyObject {
        private static final long serialVersionUID = -3118949118637372130L;
        private Batch batch;

        public RubyAckedBatch(Ruby runtime, RubyClass klass) {
            super(runtime, klass);
            this.batch = null;
        }

        public RubyAckedBatch(Ruby runtime, Batch batch) {
            super(runtime, RubyUtil.LOGSTASH_MODULE.getClass("AckedBatch"));
            this.batch = batch;
        }

        @SuppressWarnings("unchecked") // for the getList() calls
        @JRubyMethod(name = "initialize", required = 3)
        public IRubyObject ruby_initialize(ThreadContext context, IRubyObject events,  IRubyObject seqNums,  IRubyObject queue)
        {
            if (! (events instanceof RubyArray)) {
                context.runtime.newArgumentError("expected events array");
            }
            if (! (seqNums instanceof RubyArray)) {
                context.runtime.newArgumentError("expected seqNums array");
            }
            if (! (queue instanceof JrubyAckedQueueExtLibrary.RubyAckedQueue)) {
                context.runtime.newArgumentError("expected queue AckedQueue");
            }
            final Collection<Long> seqList = (List<Long>) seqNums;
            final LongVector seqs = new LongVector(seqList.size());
            for (final long seq : seqList) {
                seqs.add(seq);
            }
            this.batch = new Batch((List<Queueable>) events, seqs, ((JrubyAckedQueueExtLibrary.RubyAckedQueue)queue).getQueue());

            return context.nil;
        }

        @JRubyMethod(name = "get_elements")
        public IRubyObject ruby_get_elements(ThreadContext context)
        {
            RubyArray result = context.runtime.newArray();
            this.batch.getElements().forEach(e -> result.add(JrubyEventExtLibrary.RubyEvent.newRubyEvent(context.runtime, (Event)e)));

            return result;
        }

        @JRubyMethod(name = "close")
        public IRubyObject ruby_close(ThreadContext context)
        {
            try {
                this.batch.close();
            } catch (IOException e) {
                throw RubyUtil.newRubyIOError(context.runtime, e);
            }

            return context.nil;
        }
    }
}
