package org.logstash.execution.plugins;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.logstash.execution.Filter;
import org.logstash.execution.Input;
import org.logstash.execution.LogstashPlugin;
import org.logstash.execution.LsConfiguration;
import org.logstash.execution.LsContext;
import org.logstash.execution.Output;
import org.reflections.Reflections;

/**
 * Logstash Java Plugin Discovery.
 */
public final class DiscoverPlugins {

    public static Map<String, Class<?>> discoverPlugins() {
        Reflections reflections = new Reflections("");
        Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(LogstashPlugin.class);
        final Map<String, Class<?>> results = new HashMap<>();
        for (final Class<?> cls : annotated) {
            for (final Annotation annotation : cls.getAnnotations()) {
                if (annotation instanceof LogstashPlugin) {
                    results.put(((LogstashPlugin) annotation).name(), cls);
                    break;
                }
            }
        }
        return results;
    }

    public static void main(final String... args) {
        discoverPlugins().forEach((name, cls) -> {
            System.out.println(cls.getName());
            System.out.println(name);
            try {
                final Constructor<?> ctor = cls.getConstructor(LsConfiguration.class, LsContext.class);
                System.out.println("Found Ctor at : " + ctor.getName());
            } catch (final NoSuchMethodException ex) {
                throw new IllegalStateException(ex);
            }
            if (Filter.class.isAssignableFrom(cls)) {
                System.out.println("Filter");
            }
            if (Output.class.isAssignableFrom(cls)) {
                System.out.println("Output");
            }
            if (Input.class.isAssignableFrom(cls)) {
                System.out.println("Input");
            }
        });
    }
}
