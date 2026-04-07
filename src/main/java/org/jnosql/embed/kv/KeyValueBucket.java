package org.jnosql.embed.kv;

import org.jnosql.embed.storage.StorageEngine;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueBucket {

    private final String name;
    private final StorageEngine engine;
    private final Map<String, Instant> expirations;

    public KeyValueBucket(String name, StorageEngine engine) {
        this.name = name;
        this.engine = engine;
        this.expirations = new ConcurrentHashMap<>();
    }

    public String name() {
        return name;
    }

    public void put(String key, String value) {
        engine.put(name, key, value);
    }

    public void put(String key, String value, Duration ttl) {
        put(key, value);
        expirations.put(key, Instant.now().plus(ttl));
    }

    public String get(String key) {
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

    public void clear() {
        expirations.clear();
        for (var key : engine.scan(name)) {
            try {
                var doc = org.jnosql.embed.document.Document.fromJson(key);
                engine.delete(name, doc.id());
            } catch (Exception ignored) {
            }
        }
    }

    private boolean isExpired(String key) {
        var expiry = expirations.get(key);
        return expiry != null && Instant.now().isAfter(expiry);
    }
}
