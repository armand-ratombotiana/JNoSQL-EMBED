package org.jnosql.embed.quarkus;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import org.junify.db.config.JunifyDBConfig.StorageEngineType;

import java.util.Optional;

/**
 * Quarkus SmallRye Config mapping for the {@code junifydb.*} prefix.
 *
 * <p>Usage in {@code application.properties}:
 * <pre>
 * junifydb.engine=IN_MEMORY
 * junifydb.data-dir=data
 * junifydb.auto-flush=true
 * </pre>
 */
@ConfigMapping(prefix = "junifydb")
public interface JunifyDBQuarkusConfig {

    @WithDefault("IN_MEMORY")
    StorageEngineType engine();

    @WithDefault("data")
    String dataDir();

    @WithDefault("true")
    boolean autoFlush();

    @WithDefault("1000")
    int flushIntervalMs();

    /** Expose as bean-style getter for compatibility with existing recorder code. */
    default StorageEngineType getEngine() { return engine(); }
    default String getDataDir() { return dataDir(); }
    default boolean isAutoFlush() { return autoFlush(); }
    default int getFlushIntervalMs() { return flushIntervalMs(); }
}