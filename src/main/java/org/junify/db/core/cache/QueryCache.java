package org.junify.db.core.cache;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.time.Instant;

public class QueryCache {

    private final ConcurrentHashMap<String, CacheEntry> cache;
    private final int maxSize;
    private final long ttlMs;
    private final AtomicLong hits;
    private final AtomicLong misses;
    private final boolean staleWhileRevalidate;

    public QueryCache() {
        this(1000, 60000);
    }

    public QueryCache(int maxSize, long ttlMs) {
        this.maxSize = maxSize;
        this.ttlMs = ttlMs;
        this.cache = new ConcurrentHashMap<>();
        this.hits = new AtomicLong(0);
        this.misses = new AtomicLong(0);
        this.staleWhileRevalidate = false;
    }

    public Optional<Map<String, Object>> get(String key) {
        var entry = cache.get(key);
        
        if (entry == null) {
            misses.incrementAndGet();
            return Optional.empty();
        }
        
        if (entry.isExpired()) {
            cache.remove(key);
            misses.incrementAndGet();
            return Optional.empty();
        }
        
        hits.incrementAndGet();
        return Optional.of(entry.value);
    }

    public void put(String key, Map<String, Object> value) {
        if (cache.size() >= maxSize) {
            evictOldest();
        }
        
        cache.put(key, new CacheEntry(value, System.currentTimeMillis() + ttlMs));
    }

    public void put(String key, String sql, List<Map<String, Object>> rows, List<String> columns) {
        var value = Map.of(
            "rows", rows,
            "columns", columns,
            "executedAt", System.currentTimeMillis()
        );
        
        var cacheKey = sql != null ? sql : key;
        put(cacheKey, value);
    }

    public void invalidate(String table) {
        var keysToRemove = new ArrayList<String>();
        
        for (var key : cache.keySet()) {
            if (key.contains(table)) {
                keysToRemove.add(key);
            }
        }
        
        for (var key : keysToRemove) {
            cache.remove(key);
        }
    }

    public void invalidatePattern(String pattern) {
        var keysToRemove = new ArrayList<String>();
        
        for (var key : cache.keySet()) {
            if (key.contains(pattern)) {
                keysToRemove.add(key);
            }
        }
        
        for (var key : keysToRemove) {
            cache.remove(key);
        }
    }

    public void clear() {
        cache.clear();
        hits.set(0);
        misses.set(0);
    }

    private void evictOldest() {
        long oldest = Long.MAX_VALUE;
        String oldestKey = null;
        
        for (var entry : cache.entrySet()) {
            if (entry.getValue().expiresAt < oldest) {
                oldest = entry.getValue().expiresAt;
                oldestKey = entry.getKey();
            }
        }
        
        if (oldestKey != null) {
            cache.remove(oldestKey);
        }
    }

    public Map<String, Object> getStats() {
        return Map.of(
            "size", cache.size(),
            "maxSize", maxSize,
            "hits", hits.get(),
            "misses", misses.get(),
            "hitRatio", cache.size() > 0 ? 
                String.format("%.2f%%", (hits.get() * 100.0 / (hits.get() + misses.get()))) : "0%",
            "ttlMs", ttlMs
        );
    }

    public List<String> getCachedQueries() {
        return cache.keySet().stream().toList();
    }

    private class CacheEntry {
        final Map<String, Object> value;
        final long expiresAt;

        CacheEntry(Map<String, Object> value, long expiresAt) {
            this.value = value;
            this.expiresAt = expiresAt;
        }

        boolean isExpired() {
            return System.currentTimeMillis() > expiresAt;
        }
    }

    public static class Stats {
        public static class QueryCache {
            private final ConcurrentHashMap<String, CacheEntry> cache;
            private final AtomicLong hits;
            private final AtomicLong misses;

            public QueryCache(ConcurrentHashMap<String, CacheEntry> cache, AtomicLong hits, AtomicLong misses) {
                this.cache = cache;
                this.hits = hits;
                this.misses = misses;
            }
        }
    }
}