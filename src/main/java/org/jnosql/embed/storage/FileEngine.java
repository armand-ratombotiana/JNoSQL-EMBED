package org.jnosql.embed.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FileEngine implements StorageEngine {

    private final Path dataDir;
    private final ConcurrentMap<String, ConcurrentMap<String, String>> store;

    public FileEngine(Path dataDir) {
        this.dataDir = dataDir;
        this.store = new ConcurrentHashMap<>();
        try {
            Files.createDirectories(dataDir);
            loadAll();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize file engine", e);
        }
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
    public synchronized void flush() {
        for (var entry : store.entrySet()) {
            try {
                var file = dataDir.resolve(entry.getKey() + ".json");
                var json = org.jnosql.embed.util.JsonSerde.toJson(entry.getValue());
                Files.writeString(file, json);
            } catch (IOException e) {
                throw new RuntimeException("Failed to flush collection: " + entry.getKey(), e);
            }
        }
    }

    @Override
    public void close() {
        flush();
        store.clear();
    }

    @SuppressWarnings("unchecked")
    private void loadAll() throws IOException {
        try (var stream = Files.list(dataDir)) {
            stream.filter(p -> p.toString().endsWith(".json"))
                    .forEach(file -> {
                        try {
                            var content = Files.readString(file);
                            var map = org.jnosql.embed.util.JsonSerde.fromJson(content, ConcurrentHashMap.class);
                            var name = file.getFileName().toString().replace(".json", "");
                            store.put(name, new ConcurrentHashMap<>(map));
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to load: " + file, e);
                        }
                    });
        }
    }
}
