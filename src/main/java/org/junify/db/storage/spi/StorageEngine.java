package org.junify.db.storage.spi;

import org.junify.db.core.record.UnifiedRecord;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public interface StorageEngine {

    String name();

    void put(String collection, String key, String value);

    void putAll(String collection, Map<String, String> entries);

    String get(String collection, String key);

    List<String> getAll(String collection, List<String> keys);

    void delete(String collection, String key);

    void deleteAll(String collection, List<String> keys);

    boolean exists(String collection, String key);

    List<String> scan(String collection);

    List<String> scan(String collection, Predicate<String> filter);

    Set<String> keys(String collection);

    int size();

    Map<String, Object> stats();

    void flush();

    void close();

    /**
     * Java 25: Store a UnifiedRecord directly.
     * Default implementation serializes to JSON and delegates to put().
     */
    default void putRecord(String collection, UnifiedRecord record) {
        put(collection, record.id(), record.toJson());
    }

    /**
     * Java 25: Retrieve a UnifiedRecord by ID.
     * Default implementation delegates to get() and deserializes.
     * Implementations should override for efficient binary retrieval.
     */
    default UnifiedRecord getRecord(String collection, String id, java.util.function.Function<String, ? extends UnifiedRecord> factory) {
        var json = get(collection, id);
        return json != null ? factory.apply(json) : null;
    }
}
