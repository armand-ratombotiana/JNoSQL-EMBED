package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;

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
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> (String) row.get("TABLE_NAME"))
            .toList();
    }

    public List<ColumnInfo> getColumns(String tableName) {
        var result = engine.executeSql(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, " +
            "CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE " +
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = UPPER('" + tableName + "') " +
            "ORDER BY ORDINAL_POSITION"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        List<ColumnInfo> columns = new ArrayList<>();
        for (var row : result.rows()) {
            columns.add(new ColumnInfo(
                (String) row.get("COLUMN_NAME"),
                (String) row.get("DATA_TYPE"),
                "YES".equals(row.get("IS_NULLABLE")),
                row.get("COLUMN_DEFAULT"),
                row.get("CHARACTER_MAXIMUM_LENGTH") != null ? 
                    ((Number) row.get("CHARACTER_MAXIMUM_LENGTH")).intValue() : null,
                row.get("NUMERIC_PRECISION") != null ?
                    ((Number) row.get("NUMERIC_PRECISION")).intValue() : null,
                row.get("NUMERIC_SCALE") != null ?
                    ((Number) row.get("NUMERIC_SCALE")).intValue() : null
            ));
        }
        return columns;
    }

    public List<IndexInfo> getIndexes(String tableName) {
        var result = engine.executeSql(
            "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE " +
            "FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME = UPPER('" + tableName + "') " +
            "AND INDEX_NAME NOT LIKE 'SYS%'"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> new IndexInfo(
                (String) row.get("INDEX_NAME"),
                (String) row.get("COLUMN_NAME"),
                row.get("NON_UNIQUE") == null || (Integer) row.get("NON_UNIQUE") == 0
            ))
            .toList();
    }

    public List<ForeignKeyInfo> getForeignKeys(String tableName) {
        var result = engine.executeSql(
            "SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME " +
            "FROM INFORMATION_SCHEMA.CROSS_REFERENCES WHERE UPPER(TABLE_NAME) = UPPER('" + tableName + "')"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> new ForeignKeyInfo(
                (String) row.get("CONSTRAINT_NAME"),
                (String) row.get("COLUMN_NAME"),
                (String) row.get("REFERENCED_TABLE_NAME"),
                (String) row.get("REFERENCED_COLUMN_NAME")
            ))
            .toList();
    }

    public SqlResult createTable(String tableName, Map<String, ColumnDef> columns) {
        var sb = new StringBuilder("CREATE TABLE ");
        sb.append(tableName).append(" (\n");
        
        var colDefs = new ArrayList<String>();
        var pkCols = new ArrayList<String>();
        
        for (var entry : columns.entrySet()) {
            var col = entry.getValue();
            var colDef = new StringBuilder();
            colDef.append(entry.getKey()).append(" ").append(col.type());
            
            if (col.primaryKey()) {
                pkCols.add(entry.getKey());
            }
            if (!col.nullable()) {
                colDef.append(" NOT NULL");
            }
            if (col.defaultValue() != null) {
                colDef.append(" DEFAULT ").append(col.defaultValue());
            }
            if (col.check() != null) {
                colDef.append(" CHECK (").append(col.check()).append(")");
            }
            colDefs.add(colDef.toString());
        }
        
        if (!pkCols.isEmpty()) {
            colDefs.add("PRIMARY KEY (" + String.join(", ", pkCols) + ")");
        }
        
        sb.append(String.join(",\n", colDefs));
        sb.append(")");
        
        return engine.executeSql(sb.toString());
    }

    public SqlResult dropTable(String tableName, boolean cascade) {
        return engine.executeSql("DROP TABLE " + tableName + (cascade ? " CASCADE" : " RESTRICT"));
    }

    public SqlResult alterTableAddColumn(String tableName, ColumnDef column) {
        return engine.executeSql(
            "ALTER TABLE " + tableName + " ADD COLUMN " + 
            column.name() + " " + column.type() + 
            (column.nullable() ? "" : " NOT NULL")
        );
    }

    public SqlResult alterTableDropColumn(String tableName, String columnName) {
        return engine.executeSql("ALTER TABLE " + tableName + " DROP COLUMN " + columnName);
    }

    public SqlResult renameTable(String oldName, String newName) {
        return engine.executeSql("ALTER TABLE " + oldName + " RENAME TO " + newName);
    }

    public SqlResult createIndex(String indexName, String tableName, String... columns) {
        return engine.executeSql(
            "CREATE INDEX " + indexName + " ON " + tableName + 
            " (" + String.join(", ", columns) + ")"
        );
    }

    public SqlResult dropIndex(String indexName) {
        return engine.executeSql("DROP INDEX " + indexName);
    }

    public SqlResult analyzeTable(String tableName) {
        return engine.executeSql("ANALYZE TABLE " + tableName);
    }

    public SqlResult vacuumDatabase() {
        return engine.executeSql("VACUUM");
    }

    public Map<String, Object> getTableInfo(String tableName) {
        var info = new HashMap<String, Object>();
        info.put("exists", tableExists(tableName));
        
        if (tableExists(tableName)) {
            var result = engine.executeSql("SELECT COUNT(*) FROM " + tableName);
            if (result.success()) {
                info.put("rowCount", result.affected());
            }
            info.put("columns", getColumns(tableName));
            info.put("indexes", getIndexes(tableName));
            info.put("foreignKeys", getForeignKeys(tableName));
        }
        
        return info;
    }

    public record ColumnDef(String name, String type, boolean nullable, 
                   boolean primaryKey, String defaultValue, String check) {
        public static ColumnDef of(String name, String type) {
            return new ColumnDef(name, type, true, false, null, null);
        }
        public ColumnDef notNull() { return new ColumnDef(name, type, false, primaryKey, defaultValue, check); }
        public ColumnDef primaryKey() { return new ColumnDef(name, type, nullable, true, defaultValue, check); }
        public ColumnDef defaults(String val) { return new ColumnDef(name, type, nullable, primaryKey, val, check); }
        public ColumnDef check(String expr) { return new ColumnDef(name, type, nullable, primaryKey, defaultValue, expr); }
    }

    public record ColumnInfo(String name, String dataType, boolean nullable, Object defaultValue, 
                      Integer length, Integer precision, Integer scale) {}

    public record IndexInfo(String name, String column, boolean unique) {}

    public record ForeignKeyInfo(String constraintName, String column, 
                       String referencedTable, String referencedColumn) {}
}