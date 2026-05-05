package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;
import java.time.*;

import static org.junify.db.storage.spi.H2StorageEngine.SqlResult;

public class DatabaseMetaManager {

    private final H2StorageEngine engine;

    public DatabaseMetaManager(H2StorageEngine engine) {
        this.engine = engine;
    }

    public Map<String, Object> getDatabaseMeta() {
        var meta = new HashMap<String, Object>();
        
        var result = engine.executeSql("SELECT DATABASE()");
        if (result.success() && result.rows() != null && !result.rows().isEmpty()) {
            meta.put("database", result.rows().get(0).get("1"));
        }
        
        result = engine.executeSql("SELECT USER()");
        if (result.success() && result.rows() != null && !result.rows().isEmpty()) {
            meta.put("user", result.rows().get(0).get("1"));
        }
        
        meta.put("type", "H2");
        meta.put("version", getH2Version());
        meta.put("schema", "PUBLIC");
        meta.put("timestamp", System.currentTimeMillis());
        
        return meta;
    }

    public Map<String, Object> getSessionInfo() {
        var info = new HashMap<String, Object>();
        
        info.put("autoCommit", engine.isAutoCommit());
        info.put("isolationLevel", engine.getTransactionIsolation());
        info.put("readOnly", false);
        try {
            info.put("catalog", connection().getCatalog());
            info.put("schema", connection().getSchema());
        } catch (SQLException e) {
            info.put("catalog", "unknown");
            info.put("schema", "unknown");
        }
        
        return info;
    }

    public SqlResult setSessionTimeZone(String zoneId) {
        return engine.executeSql("SET TIME ZONE " + zoneId);
    }

    public SqlResult setDatabaseProperty(String key, String value) {
        return engine.executeSql("SET " + key + " = " + value);
    }

    public SqlResult getDatabaseProperty(String key) {
        return engine.executeSql("GET " + key);
    }

    public List<String> getDatabaseProperties() {
        var result = engine.executeSql(
            "SELECT KEY, VALUE FROM INFORMATION_SCHEMA.SETTINGS"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> row.get("KEY") + "=" + row.get("VALUE"))
            .toList();
    }

    public SqlResult analyzeAllTables() {
        var tables = engine.schemaManager().getTables();
        var results = new ArrayList<String>();
        
        for (var table : tables) {
            var result = engine.executeSql("ANALYZE TABLE " + table);
            results.add(table + ": " + result.message());
        }
        
        return new SqlResult(true, List.of("table", "status"), results.size(),
            "Analyzed " + results.size() + " tables",
            results.stream().map(r -> (Map<String, Object>) Map.of("table", (Object) r)).toList(),
            List.of("table"));
    }

    public SqlResult optimizeAllTables() {
        var tables = engine.schemaManager().getTables();
        var results = new ArrayList<String>();
        
        for (var table : tables) {
            var result = engine.executeSql("OPTIMIZE TABLE " + table);
            results.add(table + ": " + result.message());
        }
        
        return new SqlResult(true, List.of("table", "status"), results.size(),
            "Optimized " + results.size() + " tables",
            results.stream().map(r -> (Map<String, Object>) Map.of("table", (Object) r)).toList(),
            List.of("table"));
    }

    public SqlResult shutdownCompactly() {
        engine.close();
        return new SqlResult(true, null, 0, "Database compacted and closed");
    }

    public SqlResult checkpoint() {
        return engine.executeSql("CHECKPOINT");
    }

    public SqlResult compact() {
        return engine.executeSql("COMPACT");
    }

    public SqlResult traceLevel(int level) {
        return engine.executeSql("TRACE LEVEL " + level);
    }

    public SqlResult getSystemInfo() {
        return new SqlResult(true, 
            List.of("metric", "value"), 5,
            "System info",
            List.of(
                Map.of("metric", "javaVersion", "value", System.getProperty("java.version")),
                Map.of("metric", "osName", "value", System.getProperty("os.name")),
                Map.of("metric", "osArch", "value", System.getProperty("os.arch")),
                Map.of("metric", "h2Version", "value", getH2Version()),
                Map.of("metric", "timestamp", "value", Instant.now().toString())
            ),
            List.of("metric", "value"));
    }

    private String getH2Version() {
        try {
            var result = engine.executeSql(
                "SELECT H2VERSION()"
            );
            if (result.success() && result.rows() != null && !result.rows().isEmpty()) {
                return String.valueOf(result.rows().get(0).get("1"));
            }
        } catch (Exception ignored) {}
        return "unknown";
    }

    private Connection connection() {
        try {
            var field = H2StorageEngine.class.getDeclaredField("connection");
            field.setAccessible(true);
            return (Connection) field.get(engine);
        } catch (Exception e) {
            throw new RuntimeException("Cannot access connection", e);
        }
    }

    }