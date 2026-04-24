package org.junify.db.spring.boot;

import org.junify.db.config.JunifyDBConfig.StorageEngineType;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "junifydb")
public class JunifyDBProperties {

    private boolean enabled = true;
    private StorageEngineType storageEngine = StorageEngineType.IN_MEMORY;
    private String dataDir = "data";
    private boolean autoFlush = true;
    private int flushIntervalMs = 1000;
    private Integer maxPoolSize = 10;
    private String apiKey;
    private Integer port = 8080;
    private boolean enableServer = false;
    private String corsAllowedOrigins = "*";
    private boolean corsEnabled = true;
    private Integer rateLimit = 1000;
    private String defaultUsername = "sa";
    private String defaultPassword = "";

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public StorageEngineType getStorageEngine() { return storageEngine; }
    public void setStorageEngine(StorageEngineType storageEngine) { this.storageEngine = storageEngine; }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public boolean isAutoFlush() { return autoFlush; }
    public void setAutoFlush(boolean autoFlush) { this.autoFlush = autoFlush; }

    public int getFlushIntervalMs() { return flushIntervalMs; }
    public void setFlushIntervalMs(int flushIntervalMs) { this.flushIntervalMs = flushIntervalMs; }

    public Integer getMaxPoolSize() { return maxPoolSize; }
    public void setMaxPoolSize(Integer maxPoolSize) { this.maxPoolSize = maxPoolSize; }

    public String getApiKey() { return apiKey; }
    public void setApiKey(String apiKey) { this.apiKey = apiKey; }

    public Integer getPort() { return port; }
    public void setPort(Integer port) { this.port = port; }

    public boolean isEnableServer() { return enableServer; }
    public void setEnableServer(boolean enableServer) { this.enableServer = enableServer; }

    public String getCorsAllowedOrigins() { return corsAllowedOrigins; }
    public void setCorsAllowedOrigins(String corsAllowedOrigins) { this.corsAllowedOrigins = corsAllowedOrigins; }

    public boolean isCorsEnabled() { return corsEnabled; }
    public void setCorsEnabled(boolean corsEnabled) { this.corsEnabled = corsEnabled; }

    public Integer getRateLimit() { return rateLimit; }
    public void setRateLimit(Integer rateLimit) { this.rateLimit = rateLimit; }

    public String getDefaultUsername() { return defaultUsername; }
    public void setDefaultUsername(String defaultUsername) { this.defaultUsername = defaultUsername; }

    public String getDefaultPassword() { return defaultPassword; }
    public void setDefaultPassword(String defaultPassword) { this.defaultPassword = defaultPassword; }
}