package org.logstash.config.ir.expression;

import java.nio.charset.StandardCharsets;
import org.joni.Option;
import org.joni.Regex;
import org.logstash.common.SourceWithMetadata;
import org.logstash.config.ir.InvalidIRException;
import org.logstash.config.ir.SourceComponent;

/**
 * Created by andrewvc on 9/15/16.
 */
public final class RegexValueExpression extends ValueExpression {
    private final Regex regex;

    public RegexValueExpression(final SourceWithMetadata meta, final Object value)
        throws InvalidIRException {
        super(meta, value);
        if (!(value instanceof String)) {
            throw new InvalidIRException("Regex value expressions can only take strings!");
        }
        final byte[] patternBytes = getSource().getBytes(StandardCharsets.UTF_8);
        this.regex = new Regex(patternBytes, 0, patternBytes.length, Option.NONE);
    }

    @Override
    public Object get() {
        return this.regex;
    }

    public String getSource() {
        return (String) value;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public boolean sourceComponentEquals(final SourceComponent other) {
        return other instanceof RegexValueExpression &&
            ((RegexValueExpression) other).getSource().equals(getSource());
    }

    @Override
    public String toRubyString() {
        return (String) value;
    }
}
