package org.logstash;

import java.util.ArrayList;
import java.util.List;

public class FieldReference {

    private static final ThreadLocal<List<String>> PART_BUFFER =
        new ThreadLocal<List<String>>() {
            @Override
            protected List<String> initialValue() {
                return new ArrayList<>(5);
            }

            @Override
            public List<String> get() {
                List<String> b = super.get();
                b.clear(); // clear/reset the buffer
                return b;
            }

        };

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private String[] path;

    private FieldReference(String[] path) {
        this.path = path;
    }

    public String[] getPath() {
        return path;
    }

    public String getKey() {
        return path[path.length - 1];
    }

    public static FieldReference parse(final String reference) {
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
        return new FieldReference(retpath);
    }
}
