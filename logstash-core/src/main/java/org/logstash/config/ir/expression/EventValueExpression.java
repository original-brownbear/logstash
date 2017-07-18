package org.logstash.config.ir.expression;

import org.logstash.common.SourceWithMetadata;
import org.logstash.config.ir.SourceComponent;

/**
 * Created by andrewvc on 9/13/16.
 */
public class EventValueExpression extends Expression {
    private final String fieldName;

    public EventValueExpression(SourceWithMetadata meta, String fieldName) {
        super(meta);
        this.fieldName = fieldName;
    }

    private String getFieldName() {
        return fieldName;
    }

    @Override
    public boolean sourceComponentEquals(SourceComponent sourceComponent) {
        if (sourceComponent == null) return false;
        if (this == sourceComponent) return true;
        if (sourceComponent instanceof EventValueExpression) {
            return this.getFieldName().equals(((EventValueExpression) sourceComponent).fieldName);
        }
        return false;
    }

    @Override
    public String toString() {
        return "event.get('" + fieldName + "')";
    }

    @Override
    public String toRubyString() {
        return "event.getField('" + fieldName + "')";
    }

    @Override
    public String hashSource() {
        return this.getClass().getCanonicalName() + "|" + fieldName;
    }
}
