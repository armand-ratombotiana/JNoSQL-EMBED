package org.junify.db.quarkus;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import org.junify.db.config.JunifyDBConfig.StorageEngineType;

/**
 * Quarkus SmallRye Config mapping for the {@code junifydb.*} configuration prefix.
 *
 * <p>Usage in {@code application.properties}:
 * <pre>
 * junifydb.engine=IN_MEMORY
 * junifydb.data-dir=data
 * junifydb.auto-flush=true
 * junifydb.flush-interval-ms=1000
 * </pre>
 */
@ConfigMapping(prefix = "junifydb")
public interface JunifyConfig {

    @WithDefault("IN_MEMORY")
    StorageEngineType engine();

    @WithDefault("data")
    String dataDir();

    @WithDefault("true")
    boolean autoFlush();

    @WithDefault("1000")
    int flushIntervalMs();

    default StorageEngineType getEngine() { return engine(); }
    default String getDataDir() { return dataDir(); }
    default boolean isAutoFlush() { return autoFlush(); }
    default int getFlushIntervalMs() { return flushIntervalMs(); }
}
