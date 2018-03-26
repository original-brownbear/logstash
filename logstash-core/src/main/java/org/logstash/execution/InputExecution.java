package org.logstash.execution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.RubyUtil;

public final class InputExecution {

    private static final Logger LOGGER = LogManager.getLogger(InputExecution.class);

    private final Collection<IRubyObject> rubyInputs;

    private final ExecutorService executor;

    public InputExecution(final Collection<IRubyObject> rubyInputs) {
        this.rubyInputs = filterThreadable(rubyInputs);
        executor = Executors.newFixedThreadPool(this.rubyInputs.size());
    }

    public Collection<IRubyObject> inputs() {
        return Collections.unmodifiableCollection(rubyInputs);
    }

    public void start() {
        final Collection<IRubyObject> registered = new ArrayList<>();
        final ThreadContext context = RubyUtil.RUBY.getCurrentContext();
        try {
            rubyInputs.forEach(input -> {
                input.callMethod(context, "register");
                registered.add(input);
            });
        } catch (final Exception ex) {
            registered.forEach(reg -> reg.callMethod(context, "do_close"));
            throw new IllegalStateException(ex);
        }
        //TODO: Run in Threadpool and do java_pipeline.rb#inputworker things
    }

    public void stop() {
        //TODO: Add logging keys
        LOGGER.debug("Closing inputs");
        rubyInputs.forEach(input -> input.callMethod(RubyUtil.RUBY.getCurrentContext(), "do_stop"));
        executor.shutdown();
        LOGGER.debug("Closed inputs");
    }

    public void awaitStop() throws InterruptedException {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    private static Collection<IRubyObject> filterThreadable(final Collection<IRubyObject> inputs) {
        final Collection<IRubyObject> filtered = new ArrayList<>();
        final ThreadContext context = RubyUtil.RUBY.getCurrentContext();
        inputs.forEach(
            input -> {
                filtered.add(input);
                if (input.callMethod(context, "threadable").isTrue()) {
                    final int threads =
                        input.callMethod(context, "threads").convertToInteger().getIntValue();
                    if (threads > 1) {
                        for (int i = 0; i < threads - 1; ++i) {
                            filtered.add(input.callMethod(context, "clone"));
                        }
                    }
                }
            }
        );
        return filtered;
    }
}
