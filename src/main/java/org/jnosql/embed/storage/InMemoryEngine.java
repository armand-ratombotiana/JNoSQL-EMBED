package org.jnosql.embed.storage;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class InMemoryEngine implements StorageEngine {

    private final ConcurrentMap<String, ConcurrentMap<String, String>> store;

    public InMemoryEngine() {
        this.store = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String collection, String key, String value) {
        store.computeIfAbsent(collection, k -> new ConcurrentHashMap<>()).put(key, value);
    }

    @Override
    public String get(String collection, String key) {
        var col = store.get(collection);
        return col != null ? col.get(key) : null;
    }

    @Override
    public void delete(String collection, String key) {
        var col = store.get(collection);
        if (col != null) {
            col.remove(key);
        }
    }

    @Override
    public boolean exists(String collection, String key) {
        var col = store.get(collection);
        return col != null && col.containsKey(key);
    }

    @Override
    public List<String> scan(String collection) {
        var col = store.get(collection);
        return col != null ? List.copyOf(col.values()) : List.of();
    }

    @Override
    public List<String> scan(String collection, Predicate<String> filter) {
        return scan(collection).stream().filter(filter).collect(Collectors.toList());
    }

    @Override
    public java.util.Set<String> keys(String collection) {
        var col = store.get(collection);
        return col != null ? Set.copyOf(col.keySet()) : Set.of();
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
        store.clear();
    }

    public ConcurrentMap<String, ConcurrentMap<String, String>> rawStore() {
        return store;
    }
}
