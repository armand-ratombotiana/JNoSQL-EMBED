package org.jnosql.embed.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public interface StorageEngine {

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

    void flush();

    void close();
}
