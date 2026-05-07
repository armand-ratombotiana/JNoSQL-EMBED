package org.junify.db.storage.spi;

import org.junify.db.core.util.ChecksumUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class InMemoryEngine implements StorageEngine {

    private final ConcurrentMap<String, ConcurrentMap<String, String>> store;
    private final ConcurrentMap<String, ConcurrentMap<String, Long>> checksums;

    public InMemoryEngine() {
        this.store = new ConcurrentHashMap<>();
        this.checksums = new ConcurrentHashMap<>();
    }

    @Override
    public String name() {
        return "IN_MEMORY";
    }

    @Override
    public void put(String collection, String key, String value) {
        store.computeIfAbsent(collection, k -> new ConcurrentHashMap<>()).put(key, value);
        // Calculate and store checksum for data integrity
        checksums.computeIfAbsent(collection, k -> new ConcurrentHashMap<>())
                 .put(key, ChecksumUtil.calculate(value));
    }

    @Override
    public void putAll(String collection, Map<String, String> entries) {
        var col = store.computeIfAbsent(collection, k -> new ConcurrentHashMap<>());
        var checksumCol = checksums.computeIfAbsent(collection, k -> new ConcurrentHashMap<>());
        col.putAll(entries);
        // Calculate checksums for all entries
        for (var entry : entries.entrySet()) {
            checksumCol.put(entry.getKey(), ChecksumUtil.calculate(entry.getValue()));
        }
    }

    @Override
    public String get(String collection, String key) {
        var col = store.get(collection);
        if (col == null) return null;
        
        String value = col.get(key);
        if (value == null) return null;
        
        // Verify checksum on read
        var checksumCol = checksums.get(collection);
        if (checksumCol != null) {
            Long expectedChecksum = checksumCol.get(key);
            if (expectedChecksum != null && !ChecksumUtil.verify(value, expectedChecksum)) {
                System.err.println("Checksum mismatch for " + collection + ":" + key);
                throw new RuntimeException("Data corruption detected: checksum mismatch");
            }
        }
        
        return value;
    }

    @Override
    public List<String> getAll(String collection, List<String> keys) {
        var col = store.get(collection);
        if (col == null) return List.of();
        
        var results = new ArrayList<String>();
        for (var key : keys) {
            results.add(col.get(key));
        }
        return results;
    }

    @Override
    public void delete(String collection, String key) {
        var col = store.get(collection);
        if (col != null) {
            col.remove(key);
        }
        // Remove checksum as well
        var checksumCol = checksums.get(collection);
        if (checksumCol != null) {
            checksumCol.remove(key);
        }
    }

    @Override
    public void deleteAll(String collection, List<String> keys) {
        var col = store.get(collection);
        if (col != null) {
            for (var key : keys) {
                col.remove(key);
            }
        }
        // Remove checksums as well
        var checksumCol = checksums.get(collection);
        if (checksumCol != null) {
            for (var key : keys) {
                checksumCol.remove(key);
            }
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
    public Set<String> keys(String collection) {
        var col = store.get(collection);
        return col != null ? Set.copyOf(col.keySet()) : Set.of();
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
        store.clear();
        checksums.clear();
    }

    @Override
    public int size() {
        return store.values().stream().mapToInt(ConcurrentMap::size).sum();
    }

    @Override
    public Map<String, Object> stats() {
        long totalChecksums = checksums.values().stream()
            .mapToLong(ConcurrentMap::size)
            .sum();
        return Map.of(
            "engine", name(),
            "collections", store.size(),
            "totalEntries", size(),
            "type", "in-memory",
            "checksumEnabled", true,
            "checksumCount", totalChecksums
        );
    }

    public ConcurrentMap<String, ConcurrentMap<String, String>> rawStore() {
        return store;
    }
}
