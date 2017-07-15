package org.logstash;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Accessors {

    private final Map<String, Object> data;

    protected final Map<String, Object> lut;

    public Accessors(Map data) {
        this.data = data;
        this.lut = new HashMap<>(); // reference -> target LUT
    }

    public Object get(String reference) {
        final String[] field = PathCache.cache(reference);
        Object target = findTarget(reference, field);
        return target == null ? null : fetch(target, field[field.length - 1]);
    }

    public Object set(String reference, Object value) {
        final String[] field = PathCache.cache(reference);
        return store(findCreateTarget(reference, field), field[field.length - 1], value);
    }

    public Object del(String reference) {
        final String[] field = PathCache.cache(reference);
        final String key = field[field.length - 1];
        final Object target = findTarget(reference, field);
        if (target != null) {
            if (target instanceof Map) {
                return ((Map<String, Object>) target).remove(key);
            } else if (target instanceof List) {
                final int offset = listIndex(Integer.parseInt(key), ((List) target).size());
                if (offset < 0) {
                    return null;
                }
                return ((List) target).remove(offset);
            } else {
                throw newCollectionException(target);
            }
        }
        return null;
    }

    public boolean includes(String reference) {
        final String[] field = PathCache.cache(reference);
        final String key = field[field.length - 1];
        final Object target = findTarget(reference, field);
        if (target instanceof Map && ((Map<String, Object>) target).containsKey(key)) {
            return true;
        } else if (target instanceof List) {
            try {
                return foundInList((List<Object>) target, Integer.parseInt(key));
            } catch (NumberFormatException e) {
                return false;
            }
        } else {
            return false;
        }
    }

    private Object findTarget( final String reference, final String[] field) {
        Object target;

        if ((target = this.lut.get(reference)) != null) {
            return target;
        }

        target = this.data;
        for (int i = 0; i < field.length - 1; i++) {
            final String key = field[i];
            target = fetch(target, key);
            if (!(target instanceof Map || target instanceof List)) {
                return null;
            }
        }

        this.lut.put(reference, target);

        return target;
    }

    private Object findCreateTarget(final String reference, final String[] field) {
        // flush the @lut to prevent stale cached fieldref which may point to an old target
        // which was overwritten with a new value. for example, if "[a][b]" is cached and we
        // set a new value for "[a]" then reading again "[a][b]" would point in a stale target.
        // flushing the complete @lut is suboptimal, but a hierarchical lut would be required
        // to be able to invalidate fieldrefs from a common root.
        // see https://github.com/elastic/logstash/pull/5132
        this.lut.clear();
        Object target = this.data;
        for (int j = 0; j < field.length - 1; j++) {
            final String key = field[j];
            Object result = fetch(target, key);
            if (result == null) {
                result = new HashMap<String, Object>();
                if (target instanceof Map) {
                    ((Map<String, Object>) target).put(key, result);
                } else if (target instanceof List) {
                    // TODO: what about index out of bound?
                    ((List<Object>) target).set(Integer.parseInt(key), result);
                } else if (target != null) {
                    throw newCollectionException(target);
                }
            }
            target = result;
        }

        this.lut.put(reference, target);

        return target;
    }

    private static boolean foundInList(final List<Object> target, final int index) {
        final int offset = listIndex(index, target.size());
        return offset >= 0 && target.get(offset) != null;
    }

    private static Object fetch(Object target, String key) {
        if (target instanceof Map) {
            Object result = ((Map<String, Object>) target).get(key);
            return result;
        } else if (target instanceof List) {
            return fetchFromList((List<Object>) target, key);
        } else if (target == null) {
            return null;
        } else {
            throw newCollectionException(target);
        }
    }

    private static Object fetchFromList(final List<Object> list, final String key) {
        final int offset = listIndex(Integer.parseInt(key), list.size());
        if (offset < 0) {
            return null;
        }
        return list.get(offset);
    }

    private static Object store(Object target, String key, Object value) {
        if (target instanceof Map) {
            ((Map<String, Object>) target).put(key, value);
        } else if (target instanceof List) {
            return storeToList((List<Object>) target, key, value);
        } else {
            throw newCollectionException(target);
        }
        return value;
    }

    private static Object storeToList(final List<Object> target, final String key,
        final Object value) {
        final int i = Integer.parseInt(key);
        final int size = target.size();
        if (i >= size) {
            // grow array by adding trailing null items
            // this strategy reflects legacy Ruby impl behaviour and is backed by specs
            // TODO: (colin) this is potentially dangerous, and could produce OOM using arbitrary big numbers
            // TODO: (colin) should be guard against this?
            for (int j = size; j < i; j++) {
                target.add(null);
            }
            target.add(value);
        } else {
            target.set(listIndex(i, size), value);
        }
        return value;
    }

    private static ClassCastException newCollectionException(Object target) {
        return new ClassCastException("expecting List or Map, found "  + target.getClass());
    }

    /**
     * Returns a positive integer offset for a list of known size.
     *
     * @param i if positive, and offset from the start of the list. If negative, the offset from the end of the list, where -1 means the last element.
     * @param size the size of the list.
     * @return the positive integer offset for the list given by index i.
     */
    public static int listIndex(final int i, final int size) {
        if (i >= size || i < -size) {
            return -1;
        }

        if (i < 0) { // Offset from the end of the array.
            return size + i;
        } else {
            return i;
        }
    }
}
