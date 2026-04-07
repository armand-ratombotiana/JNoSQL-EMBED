package org.jnosql.embed.storage;

public interface StorageEngine {

    void put(String collection, String key, String value);

    String get(String collection, String key);

    void delete(String collection, String key);

    boolean exists(String collection, String key);

    java.util.List<String> scan(String collection);

    java.util.List<String> scan(String collection, java.util.function.Predicate<String> filter);

    java.util.Set<String> keys(String collection);

    void flush();

    void close();
}
