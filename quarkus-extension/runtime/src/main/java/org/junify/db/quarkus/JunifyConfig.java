package org.junify.db.quarkus;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import org.junify.db.config.JunifyDBConfig.StorageEngineType;

/**
 * Quarkus SmallRye Config mapping for the {@code junify.*} prefix.
 */
@ConfigMapping(prefix = "junify")
public interface JunifyConfig {

    @WithDefault("IN_MEMORY")
    StorageEngineType storageEngine();

    @WithDefault("data")
    String dataDir();

    @WithDefault("true")
    boolean autoFlush();

    default StorageEngineType getStorageEngine() { return storageEngine(); }
    default String getDataDir() { return dataDir(); }
    default boolean isAutoFlush() { return autoFlush(); }
}
