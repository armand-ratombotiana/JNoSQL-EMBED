package org.jnosql.embed.quarkus;

import io.quarkus.arc.config.ConfigProperties;
import org.jnosql.embed.config.JNoSQLConfig.StorageEngineType;

@ConfigProperties(prefix = "jnosql")
public class JNoSQLConfig {

    private StorageEngineType storageEngine = StorageEngineType.IN_MEMORY;
    private String dataDir = "data";
    private boolean autoFlush = true;

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
}
