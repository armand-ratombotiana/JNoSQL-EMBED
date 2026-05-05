package org.junify.db.quarkus;

import io.quarkus.arc.config.ConfigProperties;
import org.junify.db.config.JunifyConfig.StorageEngineType;

@ConfigProperties(prefix = "Junify")
public class JunifyConfig {

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
