package org.jnosql.embed.config;

import org.jnosql.embed.JNoSQL;
import org.jnosql.embed.storage.StorageEngine;

import java.nio.file.Path;
import java.nio.file.Paths;

public record JNoSQLConfig(
        StorageEngineType storageEngine,
        Path dataDir,
        boolean autoFlush,
        int flushIntervalMs
) {

    public enum StorageEngineType {
        IN_MEMORY,
        FILE;

        public StorageEngine create(Path dataDir, boolean autoFlush, int flushIntervalMs) {
            return switch (this) {
                case IN_MEMORY -> new org.jnosql.embed.storage.InMemoryEngine();
                case FILE -> new org.jnosql.embed.storage.FileEngine(dataDir, flushIntervalMs, autoFlush);
            };
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public JNoSQL build() {
        return JNoSQL.create(this);
    }

    public static class Builder {
        private StorageEngineType storageEngine = StorageEngineType.IN_MEMORY;
        private Path dataDir = Paths.get("data");
        private boolean autoFlush = true;
        private int flushIntervalMs = 1000;

        public Builder storageEngine(StorageEngineType engine) {
            this.storageEngine = engine;
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder storageEngine(StorageEngine engine) {
            return this;
        }

        public Builder persistTo(String path) {
            this.dataDir = Paths.get(path);
            return this;
        }

        public Builder autoFlush(boolean autoFlush) {
            this.autoFlush = autoFlush;
            return this;
        }

        public Builder flushIntervalMs(int ms) {
            this.flushIntervalMs = ms;
            return this;
        }

        public JNoSQLConfig buildConfig() {
            return new JNoSQLConfig(storageEngine, dataDir, autoFlush, flushIntervalMs);
        }

        public JNoSQL build() {
            return buildConfig().build();
        }
    }
}
