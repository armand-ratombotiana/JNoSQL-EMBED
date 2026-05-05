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

    public boolean tableExists(String tableName) {
        var result = engine.executeSql(
            "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = UPPER('" + tableName + "')"
        );
        return result.success() && result.rows() != null && !result.rows().isEmpty();
    }

    public List<String> getTables() {
        var result = engine.executeSql(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_TYPE = 'TABLE'"
        ); 
        if (!result.success() || result.rows() == null) {
            return List.of();
        }
        return result.rows().stream()
            .map(row -> (String) row.get("TABLE_NAME"))
            .collect(Collectors.toList());
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

    public List<String> getColumns(String table) {
        var sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = UPPER('" + table + "')";
        var result = engine.executeSql(sql);
        if (!result.success() || result.rows() == null) {
            return List.of();
        }
        return result.rows().stream()
            .map(row -> (String) row.get("COLUMN_NAME"))
            .collect(Collectors.toList());
    }

    public String getColumnType(String table, String column) {
        var sql = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                  "WHERE TABLE_NAME = UPPER('" + table + "') AND COLUMN_NAME = '" + column + "'";
        var result = engine.executeSql(sql);
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return "VARCHAR";
        }
        return (String) result.rows().get(0).get("DATA_TYPE");
    }

    public Integer getColumnSize(String table, String column) {
        var sql = "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS " +
                  "WHERE TABLE_NAME = UPPER('" + table + "') AND COLUMN_NAME = '" + column + "'";
        var result = engine.executeSql(sql);
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return null;
        }
        var val = result.rows().get(0).get("CHARACTER_MAXIMUM_LENGTH");
        return val != null ? (Integer) val : null;
    }

    public record TableInfo(String name, List<ColumnDef> columns) {}

    public record ColumnDef(String name, String type, Integer size) {}

    public record SqlResult(boolean success, List<String> columns, int affected, String message, 
        List<Map<String, Object>> rows, List<String> allColumns) {}

    public record CreateResult(boolean success, String message) {}

    public record DropResult(boolean success, String message) {}

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
        return new CreateResult(result.success(), result.success() ? "Table created: " + tableName : result.message());
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
        return new DropResult(result.success(), result.success() ? "Table dropped: " + tableName : result.message());
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