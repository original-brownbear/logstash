package org.logstash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class PathCache {

    private static final ThreadLocal<List<String>> PART_BUFFER =
        new ThreadLocal<List<String>>() {
            @Override
            protected List<String> initialValue() {
                return new ArrayList<>(5);
            }

            @Override
            public List<String> get() {
                final List<String> buffer = super.get();
                buffer.clear(); // clear/reset the buffer
                return buffer;
            }

        };

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private static final String BRACKETS_TIMESTAMP = "[" + Event.TIMESTAMP + "]";

    private static final String[] TIMESTAMP_PATH = parse(Event.TIMESTAMP);

    private static final ThreadLocal<Map<String, String[]>> PATH_CACHE =
        ThreadLocal.withInitial(() -> {
            final Map<String, String[]> map = new HashMap<>(100, 0.5F);
            map.put(Event.TIMESTAMP, TIMESTAMP_PATH);
            map.put(BRACKETS_TIMESTAMP, TIMESTAMP_PATH);
            return map;
        });

    private PathCache() {
    }

    public static boolean isTimestamp(final String reference) {
        return cache(reference) == TIMESTAMP_PATH;
    }

    public static String[] cache(final String reference) {
        // atomicity between the get and put is not important
        final Map<String, String[]> map = PATH_CACHE.get();
        String[] result = map.get(reference);
        if (result == null) {
            result = parse(reference);
            map.put(reference, result);
        }
        return result;
    }

    public static String[] parse(final String reference) {
        int open = reference.indexOf('[');
        int close = reference.indexOf(']', open);
        final List<String> path = PART_BUFFER.get();
        final String[] retpath;
        if (open == -1 || close == -1) {
            retpath = new String[]{reference};
        } else {
            do {
                if (open + 1 < close) {
                    path.add(reference.substring(open + 1, close));
                }
                open = reference.indexOf('[', close);
                close = reference.indexOf(']', open);
            } while (open != -1 && close != -1);
            retpath = path.toArray(EMPTY_STRING_ARRAY);
        }
        return retpath;
    }
}
