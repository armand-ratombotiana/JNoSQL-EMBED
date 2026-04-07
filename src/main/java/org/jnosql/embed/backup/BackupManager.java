package org.jnosql.embed.backup;

import org.jnosql.embed.storage.StorageEngine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class BackupManager {

    private final StorageEngine engine;

    public BackupManager(StorageEngine engine) {
        this.engine = engine;
    }

    public Path backup(Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        var backupFile = targetDir.resolve("jnosql-backup-" + System.currentTimeMillis() + ".json.gz");

        try (var out = new GZIPOutputStream(Files.newOutputStream(backupFile))) {
            var data = new java.util.LinkedHashMap<String, Object>();
            for (var key : engine.keys("")) {
                var values = engine.scan(key);
                data.put(key, values);
            }
            out.write(org.jnosql.embed.util.JsonSerde.toJson(data).getBytes());
        }

        return backupFile;
    }

    public void restore(Path backupFile) throws IOException {
        if (!Files.exists(backupFile)) {
            throw new IOException("Backup file not found: " + backupFile);
        }

        try (var in = new GZIPInputStream(Files.newInputStream(backupFile))) {
            var content = new String(in.readAllBytes());
            @SuppressWarnings("unchecked")
            var data = (java.util.Map<String, java.util.List<String>>) org.jnosql.embed.util.JsonSerde.fromJson(content, java.util.Map.class);

            for (var entry : data.entrySet()) {
                var collection = entry.getKey();
                var values = entry.getValue();
                for (var value : values) {
                    try {
                        var doc = org.jnosql.embed.document.Document.fromJson(value);
                        engine.put(collection, doc.id(), value);
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }
}
