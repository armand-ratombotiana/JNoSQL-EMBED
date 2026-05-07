package org.junify.db.nosql.kv;

import org.junify.db.core.event.EventBus;
import org.junify.db.core.metrics.DatabaseMetrics;
import org.junify.db.storage.spi.StorageEngine;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueBucket {

    private final String name;
    private final StorageEngine engine;
    private final EventBus eventBus;
    private final DatabaseMetrics metrics;
    private final Map<String, Instant> expirations;

    public KeyValueBucket(String name, StorageEngine engine, EventBus eventBus, DatabaseMetrics metrics) {
        this.name = name;
        this.engine = engine;
        this.eventBus = eventBus;
        this.metrics = metrics;
        this.expirations = new ConcurrentHashMap<>();
    }

    public String name() {
        return name;
    }

    public void put(String key, String value) {
        engine.put(name, key, value);
        metrics.recordInsert();
    }

    public void put(String key, String value, Duration ttl) {
        put(key, value);
        expirations.put(key, Instant.now().plus(ttl));
    }

    public String get(String key) {
        metrics.recordRead();
        if (isExpired(key)) {
            engine.delete(name, key);
            expirations.remove(key);
            return null;
        }
        return engine.get(name, key);
    }

    public boolean delete(String key) {
        expirations.remove(key);
        if (engine.exists(name, key)) {
            engine.delete(name, key);
            metrics.recordDelete();
            return true;
        }
        return false;
    }

    public boolean exists(String key) {
        if (isExpired(key)) {
            engine.delete(name, key);
            expirations.remove(key);
            return false;
        }
        return engine.exists(name, key);
    }

    public long increment(String key) {
        return increment(key, 1);
    }

    public long increment(String key, long delta) {
        var val = get(key);
        long current = val != null ? Long.parseLong(val) : 0;
        long next = current + delta;
        put(key, String.valueOf(next));
        return next;
    }

    public long decrement(String key) {
        return increment(key, -1);
    }

    /**
     * PHASE 2: Batch put multiple key-value pairs.
     */
    public void putAll(Map<String, String> entries) {
        engine.putAll(name, entries);
        entries.forEach((k, v) -> metrics.recordInsert());
    }

    /**
     * PHASE 2: Batch get multiple values.
     */
    public Map<String, String> getAll(Iterable<String> keys) {
        var result = new LinkedHashMap<String, String>();
        for (String key : keys) {
            var value = get(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    /**
     * PHASE 2: Get all keys in the bucket.
     */
    public Set<String> keys() {
        return engine.keys(name);
    }

    /**
     * PHASE 2: Get the count of non-expired keys.
     */
    public long count() {
        return keys().stream().filter(k -> !isExpired(k)).count();
    }

    /**
     * PHASE 2: Clear all keys in the bucket.
     */
    public void clear() {
        for (String key : keys()) {
            delete(key);
        }
        expirations.clear();
    }

    /**
     * PHASE 2: Get bucket statistics.
     */
    public Map<String, Object> stats() {
        long total = keys().size();
        long expired = expirations.entrySet().stream()
            .filter(e -> e.getValue().isBefore(Instant.now()))
            .count();
        return Map.of(
            "name", name,
            "totalKeys", total,
            "expiredKeys", expired,
            "activeKeys", total - expired,
            "memoryEntries", expirations.size()
        );
    }

    private boolean isExpired(String key) {
        var expiry = expirations.get(key);
        return expiry != null && Instant.now().isAfter(expiry);
    }
}
