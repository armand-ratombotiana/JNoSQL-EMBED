package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junify.db.storage.spi.H2StorageEngine.SqlResult;

public class SchemaManager {

    private final H2StorageEngine engine;

    public SchemaManager(H2StorageEngine engine) {
        this.engine = engine;
    }

    /**
     * Check if table exists.
     * Note: H2 stores identifiers in lowercase when DATABASE_TO_LOWER=TRUE.
     * Uses prepared statement to prevent SQL injection.
     */
    public boolean tableExists(String tableName) {
        var result = engine.executeSql(
            "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
            tableName.toLowerCase()
        );
        return result.success() && result.rows() != null && !result.rows().isEmpty();
    }

    /**
     * Get all table names.
     * Note: H2 uses TABLE_TYPE = 'BASE TABLE' for user tables.
     * Note: With DATABASE_TO_LOWER=TRUE, schema names are stored lowercase.
     * Uses prepared statement for consistent API (no user input in this query).
     */
    public List<String> getTables() {
        var result = engine.executeSql(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = ?",
            "public", "BASE TABLE"
        );
        if (!result.success() || result.rows() == null) {
            return List.of();
        }
        return result.rows().stream()
            .map(row -> (String) row.get("TABLE_NAME"))
            .filter(name -> name != null)
            .collect(Collectors.toList());
    }

    /**
     * Get full schema information for API response.
     * @return Map with tables array containing table info
     */
    public Map<String, Object> getSchemaInfo() {
        var tables = getTables();
        var tableList = tables.stream()
            .map(t -> {
                var tableMap = new java.util.HashMap<String, Object>();
                tableMap.put("name", t);
                var cols = getColumns(t).stream()
                    .map(c -> {
                        var colMap = new java.util.HashMap<String, Object>();
                        colMap.put("name", c);
                        colMap.put("type", getColumnType(t, c));
                        var size = getColumnSize(t, c);
                        if (size != null) colMap.put("size", size);
                        return colMap;
                    })
                    .collect(Collectors.toList());
                tableMap.put("columns", cols);
                return tableMap;
            })
            .collect(Collectors.toList());
        var result = new java.util.HashMap<String, Object>();
        result.put("tables", tableList);
        return result;
    }

    public SqlResult getSchema() {
        var tables = getTables();
        var schemaInfo = tables.stream()
            .map(t -> {
                var cols = getColumns(t).stream()
                    .map(c -> new ColumnDef(c, getColumnType(t, c), getColumnSize(t, c)))
                    .collect(Collectors.toList());
                return new TableInfo(t, cols);
            })
            .collect(Collectors.toList());
        return new SqlResult(true, List.of(), 0, "OK", List.of(), List.of());
    }

    /**
     * Get column names for a table.
     * Note: Uses lowercase table name for H2 compatibility.
     * Uses prepared statement to prevent SQL injection.
     */
    public List<String> getColumns(String table) {
        if (table == null) return List.of();
        var sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
        var result = engine.executeSql(sql, table.toLowerCase());
        if (!result.success() || result.rows() == null) {
            return List.of();
        }
        return result.rows().stream()
            .map(row -> {
                Object col = row.get("COLUMN_NAME");
                return col != null ? col.toString() : null;
            })
            .filter(c -> c != null)
            .collect(Collectors.toList());
    }

    /**
     * Get column data type.
     * Note: Uses lowercase table name for H2 compatibility.
     * Uses prepared statement to prevent SQL injection.
     */
    public String getColumnType(String table, String column) {
        if (table == null || column == null) return "VARCHAR";
        var sql = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                  "WHERE TABLE_NAME = ? AND COLUMN_NAME = ?";
        var result = engine.executeSql(sql, table.toLowerCase(), column);
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return "VARCHAR";
        }
        Object type = result.rows().get(0).get("DATA_TYPE");
        return type != null ? type.toString() : "VARCHAR";
    }

    /**
     * Get column size.
     * Note: Uses lowercase table name for H2 compatibility.
     * Uses prepared statement to prevent SQL injection.
     */
    public Integer getColumnSize(String table, String column) {
        var sql = "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS " +
                  "WHERE TABLE_NAME = ? AND COLUMN_NAME = ?";
        var result = engine.executeSql(sql, table.toLowerCase(), column);
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return null;
        }
        var val = result.rows().get(0).get("CHARACTER_MAXIMUM_LENGTH");
        if (val == null) return null;
        if (val instanceof Long) return ((Long) val).intValue();
        if (val instanceof Integer) return (Integer) val;
        return null;
    }

    public record TableInfo(String name, List<ColumnDef> columns) {}

    public record ColumnDef(String name, String type, Integer size) {}

    public record SqlResult(boolean success, List<String> columns, int affected, String message,
        List<Map<String, Object>> rows, List<String> allColumns) {}

    /**
     * CreateResult with error code for programmatic handling.
     */
    public record CreateResult(boolean success, String message, String errorCode, String sqlState, String suggestion) {
        public CreateResult(boolean success, String message) {
            this(success, message, success ? null : "INTERNAL_ERROR", null, null);
        }

        public CreateResult(boolean success, String message, H2StorageEngine.SqlResult sqlResult) {
            this(success, message,
                 sqlResult != null ? sqlResult.errorCode() : null,
                 sqlResult != null ? sqlResult.sqlState() : null,
                 sqlResult != null ? sqlResult.suggestion() : null);
        }

        /**
         * Convert to error map if operation failed.
         */
        public Map<String, Object> toErrorMap() {
            if (success) return null;
            Map<String, Object> error = new HashMap<>();
            error.put("code", errorCode != null ? errorCode : "INTERNAL_ERROR");
            error.put("message", message);
            if (sqlState != null) error.put("sqlState", sqlState);
            if (suggestion != null) error.put("suggestion", suggestion);
            return error;
        }
    }

    /**
     * DropResult with error code for programmatic handling.
     */
    public record DropResult(boolean success, String message, String errorCode, String sqlState, String suggestion) {
        public DropResult(boolean success, String message) {
            this(success, message, success ? null : "INTERNAL_ERROR", null, null);
        }

        public DropResult(boolean success, String message, H2StorageEngine.SqlResult sqlResult) {
            this(success, message,
                 sqlResult != null ? sqlResult.errorCode() : null,
                 sqlResult != null ? sqlResult.sqlState() : null,
                 sqlResult != null ? sqlResult.suggestion() : null);
        }

        /**
         * Convert to error map if operation failed.
         */
        public Map<String, Object> toErrorMap() {
            if (success) return null;
            Map<String, Object> error = new HashMap<>();
            error.put("code", errorCode != null ? errorCode : "INTERNAL_ERROR");
            error.put("message", message);
            if (sqlState != null) error.put("sqlState", sqlState);
            if (suggestion != null) error.put("suggestion", suggestion);
            return error;
        }
    }

    public CreateResult createTable(String tableName, Map<String, Object> columns) {
        if (tableExists(tableName)) {
            return new CreateResult(false, "Table already exists: " + tableName);
        }
        var columnDefs = new StringBuilder();
        for (var entry : columns.entrySet()) {
            if (columnDefs.length() > 0) columnDefs.append(", ");
            var colName = entry.getKey();
            var colType = entry.getValue() != null ? entry.getValue().toString() : "VARCHAR";
            columnDefs.append(colName).append(" ").append(colType);
        }
        var sql = "CREATE TABLE " + tableName + " (" + columnDefs + ")";
        var result = engine.executeSql(sql);
        return new CreateResult(result.success(),
                               result.success() ? "Table created: " + tableName : result.message(),
                               result);
    }

    public DropResult dropTable(String tableName) {
        return dropTable(tableName, false);
    }

    public DropResult dropTable(String tableName, boolean force) {
        if (!force && !tableExists(tableName)) {
            return new DropResult(false, "Table does not exist: " + tableName);
        }
        var sql = "DROP TABLE IF EXISTS " + tableName;
        var result = engine.executeSql(sql);
        return new DropResult(result.success(),
                             result.success() ? "Table dropped: " + tableName : result.message(),
                             result);
    }

    public Map<String, Object> getTableInfo(String tableName) {
        var info = new java.util.HashMap<String, Object>();
        info.put("name", tableName);
        var columns = getColumns(tableName).stream()
            .map(c -> {
                var colMap = new java.util.HashMap<String, Object>();
                colMap.put("name", c);
                colMap.put("type", getColumnType(tableName, c));
                var size = getColumnSize(tableName, c);
                if (size != null) colMap.put("size", size);
                return colMap;
            })
            .collect(Collectors.toList());
        info.put("columns", columns);
        return info;
    }
}