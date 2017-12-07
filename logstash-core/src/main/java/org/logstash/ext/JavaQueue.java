package org.logstash.ext;

import org.jruby.runtime.builtin.IRubyObject;

public final class JavaQueue {

    private final IRubyObject queue;

    public JavaQueue(final IRubyObject queue) {
        this.queue = queue;
    }

    public void push(final JrubyEventExtLibrary.RubyEvent event) {

    }
}
