package org.jnosql.embed.spring;

import org.jnosql.embed.config.JNoSQLConfig.StorageEngineType;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "jnosql")
public class JNoSQLProperties {

    private StorageEngineType storageEngine = StorageEngineType.IN_MEMORY;
    private String dataDir = "data";
    private boolean autoFlush = true;
    private int flushIntervalMs = 1000;

    public StorageEngineType getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(StorageEngineType storageEngine) {
        this.storageEngine = storageEngine;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public boolean isAutoFlush() {
        return autoFlush;
    }

    public void setAutoFlush(boolean autoFlush) {
        this.autoFlush = autoFlush;
    }

    public int getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public void setFlushIntervalMs(int flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
    }
}
