package org.junify.db.micronaut;

import io.micronaut.context.annotation.*;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;

import jakarta.inject.Singleton;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Optional;

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

    @Property(name = "junifydb.enable-server", defaultValue = "false")
    private boolean enableServer = false;

    @Property(name = "junifydb.port", defaultValue = "8080")
    private int port = 8080;

    @Property(name = "junifydb.api-key")
    private Optional<String> apiKey = Optional.empty();

    @Property(name = "junifydb.enable-cors", defaultValue = "true")
    private boolean enableCors = true;

    @Property(name = "junifydb.cors-allowed-origins", defaultValue = "*")
    private String corsAllowedOrigins = "*";

    @PostConstruct
    void initialize() {
        if (enabled) {
            var config = JunifyDBConfig.builder()
                .storageEngine(parseEngine(engine))
                .persistTo(dataDir)
                .autoFlush(autoFlush)
                .flushIntervalMs(flushIntervalMs)
                .enableServer(enableServer)
                .port(port)
                .apiKey(apiKey.orElse(null))
                .enableCors(enableCors)
                .corsAllowedOrigins(corsAllowedOrigins)
                .build();

            junifyDB = JunifyDB.create(config);
        }
    }

    @Singleton
    @NonNull
    public JunifyDB junifyDB() {
        if (junifyDB == null) {
            throw new IllegalStateException("JunifyDB not initialized. Check junifydb.enabled");
        }
        return junifyDB;
    }

    @Singleton
    @NonNull
    public JunifyDBConfig junifyDBConfig() {
        return JunifyDBConfig.builder()
            .storageEngine(parseEngine(engine))
            .persistTo(dataDir)
            .autoFlush(autoFlush)
            .flushIntervalMs(flushIntervalMs)
            .enableServer(enableServer)
            .port(port)
            .apiKey(apiKey.orElse(null))
            .enableCors(enableCors)
            .corsAllowedOrigins(corsAllowedOrigins)
            .build();
    }

    @PreDestroy
    void stop() {
        if (junifyDB != null && junifyDB.isOpen()) {
            junifyDB.close();
        }
    }

    private JunifyDBConfig.StorageEngineType parseEngine(String engine) {
        try {
            return JunifyDBConfig.StorageEngineType.valueOf(engine);
        } catch (Exception e) {
            return JunifyDBConfig.StorageEngineType.IN_MEMORY;
        }
    }
}