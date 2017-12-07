package org.logstash.ext;

import org.jruby.internal.runtime.methods.DynamicMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.RubyUtil;

public final class JavaQueue implements EventQueue {

    private final IRubyObject queue;

    private final DynamicMethod enqueue;

    private static final ThreadLocal<IRubyObject[]> ARGS =
        ThreadLocal.withInitial(() -> new IRubyObject[1]);

    public JavaQueue(final IRubyObject queue) {
        this.queue = queue;
        enqueue = queue.getMetaClass().searchMethod("push");
    }

    @Override
    public void push(final JrubyEventExtLibrary.RubyEvent event) {
        final IRubyObject[] arguments = ARGS.get();
        arguments[0] = event;
        enqueue.call(
            RubyUtil.RUBY.getCurrentContext(), queue, RubyUtil.LOGSTASH_MODULE, "push",
            arguments, Block.NULL_BLOCK
        );
    }
}
