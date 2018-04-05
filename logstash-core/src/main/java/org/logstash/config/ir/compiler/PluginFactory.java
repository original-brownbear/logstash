package org.logstash.config.ir.compiler;

import org.logstash.execution.LsConfiguration;
import org.logstash.execution.LsContext;

/**
 * Factory that can instantiate Java plugins as well as Ruby plugins.
 */
public interface PluginFactory extends RubyIntegration.PluginFactory {

    org.logstash.execution.Filter buildFilter(
        String name, String id, LsConfiguration configuration, LsContext context
    );
}
