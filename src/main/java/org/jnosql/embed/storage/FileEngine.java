package org.jnosql.embed.storage;

import org.jnosql.embed.util.JsonSerde;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FileEngine implements StorageEngine {

    private final Path dataDir;
    private final ConcurrentMap<String, ConcurrentMap<String, String>> store;
    private final ExecutorService writer;
    private final AtomicBoolean dirty = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler;
    private final boolean asyncEnabled;
    private final WriteAheadLog wal;

    public FileEngine(Path dataDir) {
        this(dataDir, 1000, true);
    }

    public FileEngine(Path dataDir, long flushIntervalMs) {
        this(dataDir, flushIntervalMs, true);
    }

    public FileEngine(Path dataDir, long flushIntervalMs, boolean asyncEnabled) {
        this.dataDir = dataDir;
        this.store = new ConcurrentHashMap<>();
        this.asyncEnabled = asyncEnabled;
        
        try {
            this.wal = new WriteAheadLog(dataDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize WAL", e);
        }
        
        if (asyncEnabled) {
            this.writer = Executors.newSingleThreadExecutor();
            this.scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(this::asyncFlush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
        } else {
            this.writer = null;
            this.scheduler = null;
        }
        
        try {
            Files.createDirectories(dataDir);
            loadAll();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize file engine", e);
        }
    }

    @Override
    public String name() {
        return "FILE";
    }

    @Override
    public void put(String collection, String key, String value) {
        store.computeIfAbsent(collection, k -> new ConcurrentHashMap<>()).put(key, value);
        
        wal.log("PUT", collection, key, value);
        
        if (asyncEnabled) {
            dirty.set(true);
        }
    }

    @Override
    public void putAll(String collection, Map<String, String> entries) {
        var col = store.computeIfAbsent(collection, k -> new ConcurrentHashMap<>());
        col.putAll(entries);
        
        for (var entry : entries.entrySet()) {
            wal.log("PUT", collection, entry.getKey(), entry.getValue());
        }
        
        if (asyncEnabled) {
            dirty.set(true);
        }
    }

    @Override
    public String get(String collection, String key) {
        var col = store.get(collection);
        return col != null ? col.get(key) : null;
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
            
            wal.log("DELETE", collection, key, null);
            
            if (asyncEnabled) {
                dirty.set(true);
            }
        }
    }

    @Override
    public void deleteAll(String collection, List<String> keys) {
        var col = store.get(collection);
        if (col != null) {
            for (var key : keys) {
                col.remove(key);
                wal.log("DELETE", collection, key, null);
            }
            if (asyncEnabled) {
                dirty.set(true);
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
        if (asyncEnabled) {
            asyncFlush();
            sync();
        } else {
            syncFlush();
        }
        
        try {
            wal.checkpoint();
        } catch (IOException e) {
            System.err.println("WAL checkpoint failed: " + e.getMessage());
        }
    }

    public void sync() {
        if (!asyncEnabled) return;
        var latch = new java.util.concurrent.CountDownLatch(1);
        writer.execute(() -> latch.countDown());
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private synchronized void asyncFlush() {
        if (!dirty.getAndSet(false)) return;
        
        writer.execute(() -> {
            for (var entry : store.entrySet()) {
                try {
                    var file = dataDir.resolve(entry.getKey() + ".json");
                    var json = JsonSerde.toJson(entry.getValue());
                    Files.writeString(file, json);
                } catch (IOException e) {
                    System.err.println("Failed to flush: " + entry.getKey());
                }
            }
        });
    }

    private synchronized void syncFlush() {
        for (var entry : store.entrySet()) {
            try {
                var file = dataDir.resolve(entry.getKey() + ".json");
                var json = JsonSerde.toJson(entry.getValue());
                Files.writeString(file, json);
            } catch (IOException e) {
                throw new RuntimeException("Failed to flush: " + entry.getKey(), e);
            }
        }
    }

    @Override
    public void close() {
        if (asyncEnabled) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            writer.shutdown();
            try {
                writer.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        flush();
        
        try {
            wal.close();
        } catch (IOException e) {
            System.err.println("WAL close failed: " + e.getMessage());
        }
        
        store.clear();
    }

    @Override
    public int size() {
        return store.values().stream().mapToInt(ConcurrentMap::size).sum();
    }

    @Override
    public Map<String, Object> stats() {
        return Map.of(
            "engine", name(),
            "collections", store.size(),
            "totalEntries", size(),
            "dataDir", dataDir.toString(),
            "asyncEnabled", asyncEnabled,
            "type", "file-persistent"
        );
    }

    @SuppressWarnings("unchecked")
    private void loadAll() throws IOException {
        try (var stream = Files.list(dataDir)) {
            stream.filter(p -> p.toString().endsWith(".json"))
                    .forEach(file -> {
                        try {
                            var content = Files.readString(file);
                            var map = JsonSerde.fromJson(content, ConcurrentHashMap.class);
                            var name = file.getFileName().toString().replace(".json", "");
                            store.put(name, new ConcurrentHashMap<>(map));
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to load: " + file, e);
                        }
                    });
        }
    }
}
