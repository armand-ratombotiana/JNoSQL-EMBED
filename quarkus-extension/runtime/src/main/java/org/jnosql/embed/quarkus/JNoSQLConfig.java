package org.jnosql.embed.quarkus;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import org.junify.db.config.JunifyDBConfig.StorageEngineType;

/**
 * Quarkus SmallRye Config mapping for the legacy {@code jnosql.*} prefix.
 * New applications should use {@link JunifyDBQuarkusConfig} ({@code junifydb.*}).
 */
@ConfigMapping(prefix = "jnosql")
public interface JNoSQLConfig {

    @WithDefault("IN_MEMORY")
    StorageEngineType storageEngine();

    @WithDefault("data")
    String dataDir();

    @WithDefault("true")
    boolean autoFlush();

    /** Bean-style accessors for compatibility with recorder code. */
    default StorageEngineType getStorageEngine() { return storageEngine(); }
    default String getDataDir() { return dataDir(); }
    default boolean isAutoFlush() { return autoFlush(); }
}
