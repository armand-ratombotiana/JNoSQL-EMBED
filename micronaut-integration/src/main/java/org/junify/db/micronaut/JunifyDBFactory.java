package org.junify.db.micronaut;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;

import jakarta.inject.Singleton;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Micronaut {@link Factory} that creates and manages the {@link JunifyDB} singleton.
 *
 * <p>Configuration is read from {@code application.yml} / {@code application.properties}
 * using the {@code junifydb.*} prefix:
 * <pre>
 * junifydb:
 *   enabled: true
 *   engine: IN_MEMORY          # IN_MEMORY | FILE | LSM_TREE | B_TREE
 *   data-dir: ./data/junifydb
 *   auto-flush: true
 *   flush-interval-ms: 1000
 * </pre>
 */
@Factory
public class JunifyDBFactory {

    @Nullable
    private JunifyDB junifyDB;

    @Property(name = "junifydb.enabled", defaultValue = "true")
    private boolean enabled = true;

    @Property(name = "junifydb.engine", defaultValue = "IN_MEMORY")
    private String engine = "IN_MEMORY";

    @Property(name = "junifydb.data-dir", defaultValue = "./data/junifydb")
    private String dataDir = "./data/junifydb";

    @Property(name = "junifydb.auto-flush", defaultValue = "true")
    private boolean autoFlush = true;

    @Property(name = "junifydb.flush-interval-ms", defaultValue = "1000")
    private int flushIntervalMs = 1000;

    @PostConstruct
    void initialize() {
        if (enabled) {
            var config = JunifyDBConfig.builder()
                    .storageEngine(parseEngine(engine))
                    .persistTo(dataDir)
                    .autoFlush(autoFlush)
                    .flushIntervalMs(flushIntervalMs)
                    .buildConfig();

            junifyDB = JunifyDB.create(config);
        }
    }

    @Singleton
    @NonNull
    public JunifyDB junifyDB() {
        if (junifyDB == null) {
            throw new IllegalStateException("JunifyDB is not initialized. Check that junifydb.enabled=true.");
        }
        return junifyDB;
    }

    @PreDestroy
    void stop() {
        if (junifyDB != null && junifyDB.isOpen()) {
            junifyDB.close();
        }
    }

    private JunifyDBConfig.StorageEngineType parseEngine(String engine) {
        try {
            return JunifyDBConfig.StorageEngineType.valueOf(engine.toUpperCase());
        } catch (IllegalArgumentException e) {
            return JunifyDBConfig.StorageEngineType.IN_MEMORY;
        }
    }
}