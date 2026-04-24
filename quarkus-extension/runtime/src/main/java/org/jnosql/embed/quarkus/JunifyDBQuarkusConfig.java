package org.jnosql.embed.quarkus;

import io.quarkus.arc.config.ConfigProperties;
import org.junify.db.config.JunifyDBConfig.StorageEngineType;

import java.util.Optional;

@ConfigProperties(prefix = "junifydb")
public class JunifyDBQuarkusConfig {

    private StorageEngineType engine = StorageEngineType.IN_MEMORY;
    private String dataDir = "data";
    private boolean autoFlush = true;
    private int flushIntervalMs = 1000;
    private boolean enableServer = false;
    private int port = 8080;
    private Optional<String> apiKey = Optional.empty();
    private boolean enableCors = true;
    private String corsAllowedOrigins = "*";
    private int rateLimit = 1000;

    public StorageEngineType getEngine() {
        return engine;
    }

    public void setEngine(StorageEngineType engine) {
        this.engine = engine;
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

    public boolean isEnableServer() {
        return enableServer;
    }

    public void setEnableServer(boolean enableServer) {
        this.enableServer = enableServer;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Optional<String> getApiKey() {
        return apiKey;
    }

    public void setApiKey(Optional<String> apiKey) {
        this.apiKey = apiKey;
    }

    public boolean isEnableCors() {
        return enableCors;
    }

    public void setEnableCors(boolean enableCors) {
        this.enableCors = enableCors;
    }

    public String getCorsAllowedOrigins() {
        return corsAllowedOrigins;
    }

    public void setCorsAllowedOrigins(String corsAllowedOrigins) {
        this.corsAllowedOrigins = corsAllowedOrigins;
    }

    public int getRateLimit() {
        return rateLimit;
    }

    public void setRateLimit(int rateLimit) {
        this.rateLimit = rateLimit;
    }
}