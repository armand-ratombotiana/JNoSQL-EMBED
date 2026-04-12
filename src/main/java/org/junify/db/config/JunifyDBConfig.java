package org.junify.db.config;

import org.junify.db.JunifyDB;
import org.junify.db.storage.StorageEngine;

import java.nio.file.Path;
import java.nio.file.Paths;

public record JunifyDBConfig(
        StorageEngineType storageEngine,
        Path dataDir,
        boolean autoFlush,
        int flushIntervalMs
) {

    public enum StorageEngineType {
        IN_MEMORY,
        FILE,
        LSM_TREE,
        B_TREE,
        H2;

        public StorageEngine create(Path dataDir, boolean autoFlush, int flushIntervalMs) {
            return switch (this) {
                case IN_MEMORY -> new org.junify.db.storage.InMemoryEngine();
                case FILE -> new org.junify.db.storage.FileEngine(dataDir, flushIntervalMs, autoFlush);
                case LSM_TREE -> new org.junify.db.storage.LSMTreeEngine(dataDir, 1024 * 1024, 64 * 1024 * 1024);
                case B_TREE -> new org.junify.db.storage.BTreeEngine(dataDir);
                case H2 -> new org.junify.db.storage.H2StorageEngine(dataDir);
            };
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public JunifyDB build() {
        return JunifyDB.create(this);
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

        public JunifyDBConfig buildConfig() {
            return new JunifyDBConfig(storageEngine, dataDir, autoFlush, flushIntervalMs);
        }

        public JunifyDB build() {
            return buildConfig().build();
        }
    }
}
