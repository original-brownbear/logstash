package org.logstash;

import java.util.concurrent.ConcurrentHashMap;

public final class PathCache {

    private static final ConcurrentHashMap<String, String[]> cache =
        new ConcurrentHashMap<>(10, 0.5F, 1);

    private static final String[] timestamp = cache(Event.TIMESTAMP);

    private static final String BRACKETS_TIMESTAMP = "[" + Event.TIMESTAMP + "]";

    static {
        // inject @timestamp
        cache(BRACKETS_TIMESTAMP, timestamp);
    }

    public static boolean isTimestamp(String reference) {
        return cache(reference) == timestamp;
    }

    public static String[] cache(String reference) {
        // atomicity between the get and put is not important
        String[] result = cache.get(reference);
        if (result == null) {
            result = FieldReference.parse(reference);
            cache.put(reference, result);
        }
        return result;
    }

    public static String[] cache(String reference, String[] field) {
        cache.put(reference, field);
        return field;
    }
}
