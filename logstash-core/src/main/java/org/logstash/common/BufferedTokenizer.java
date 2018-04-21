package org.logstash.common;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.RubyUtil;

@JRubyClass(name ="BufferedTokenizer")
public class BufferedTokenizer extends RubyObject {

    private IRubyObject input = RubyUtil.RUBY.newArray();

    public BufferedTokenizer(final Ruby runtime, final RubyClass metaClass) {
        super(runtime, metaClass);
    }

    /**
     * Flush the contents of the input buffer, i.e. return the input buffer even though
     * a token has not yet been encountered
     * @param context ThreadContext
     * @return Buffer
     */
    @JRubyMethod
    public IRubyObject flush(final ThreadContext context) {
        final IRubyObject buffer = input.callMethod(context, "join");
        input.callMethod(context, "clear");
        return buffer;
    }
}
