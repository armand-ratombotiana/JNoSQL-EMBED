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
}