package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;

public class ConstraintManager {

    private final H2StorageEngine engine;

    public ConstraintManager(H2StorageEngine engine) {
        this.engine = engine;
    }

    public List<ForeignKeyInfo> getForeignKeys(String tableName) {
        var result = engine.executeSql(
            "SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME " +
            "FROM INFORMATION_SCHEMA.CROSS_REFERENCES WHERE TABLE_NAME = UPPER('" + tableName + "')"
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

    public SqlResult addForeignKey(String constraintName, String column, 
                                String refTable, String refColumn) {
        var sql = "ALTER TABLE ADD CONSTRAINT " + constraintName + 
                  " FOREIGN KEY (" + column + ") REFERENCES " + refTable + 
                  " (" + refColumn + ")";
        return engine.executeSql(sql);
    }

    public SqlResult addForeignKeyOnDelete(String constraintName, String column,
                                     String refTable, String refColumn, String onDelete) {
        var sql = "ALTER TABLE ADD CONSTRAINT " + constraintName +
                  " FOREIGN KEY (" + column + ") REFERENCES " + refTable +
                  " (" + refColumn + ") ON DELETE " + onDelete;
        return engine.executeSql(sql);
    }

    public SqlResult dropForeignKey(String constraintName) {
        return engine.executeSql("ALTER TABLE DROP CONSTRAINT " + constraintName);
    }

    public List<CheckConstraintInfo> getCheckConstraints(String tableName) {
        var result = engine.executeSql(
            "SELECT CONSTRAINT_NAME, CHECK_EXPRESSION FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS " +
            "WHERE TABLE_NAME = UPPER('" + tableName + "')"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> new CheckConstraintInfo(
                (String) row.get("CONSTRAINT_NAME"),
                (String) row.get("CHECK_EXPRESSION")
            ))
            .toList();
    }

    public SqlResult addCheckConstraint(String constraintName, String tableName, String expression) {
        return engine.executeSql(
            "ALTER TABLE " + tableName + " ADD CONSTRAINT " + constraintName +
            " CHECK (" + expression + ")"
        );
    }

    public SqlResult dropCheckConstraint(String constraintName) {
        return engine.executeSql("ALTER TABLE DROP CONSTRAINT " + constraintName);
    }

    public List<UniqueConstraintInfo> getUniqueConstraints(String tableName) {
        var result = engine.executeSql(
            "SELECT DISTINCT CONSTRAINT_NAME, COLUMN_NAME " +
            "FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME = UPPER('" + tableName + "') " +
            "AND INDEX_NAME NOT LIKE 'SYS%'"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> new UniqueConstraintInfo(
                (String) row.get("CONSTRAINT_NAME"),
                (String) row.get("COLUMN_NAME"),
                true
            ))
            .toList();
    }

    public SqlResult addUniqueConstraint(String constraintName, String tableName, String... columns) {
        return engine.executeSql(
            "ALTER TABLE " + tableName + " ADD CONSTRAINT " + constraintName +
            " UNIQUE (" + String.join(", ", columns) + ")"
        );
    }

    public SqlResult addNotNullConstraint(String tableName, String column) {
        return engine.executeSql(
            "ALTER TABLE " + tableName + " ALTER COLUMN " + column + " NOT NULL"
        );
    }

    public SqlResult dropNotNullConstraint(String tableName, String column) {
        return engine.executeSql(
            "ALTER TABLE " + tableName + " ALTER COLUMN " + column + " NULL"
        );
    }

    public SqlResult addDefaultConstraint(String tableName, String column, String defaultValue) {
        return engine.executeSql(
            "ALTER TABLE " + tableName + " ALTER COLUMN " + column +
            " DEFAULT " + defaultValue
        );
    }

    public Map<String, Object> getAllConstraints(String tableName) {
        var constraints = new HashMap<String, Object>();
        constraints.put("primaryKeys", getPrimaryKey(tableName));
        constraints.put("foreignKeys", getForeignKeys(tableName));
        constraints.put("uniqueConstraints", getUniqueConstraints(tableName));
        constraints.put("checkConstraints", getCheckConstraints(tableName));
        return constraints;
    }

    private String getPrimaryKey(String tableName) {
        var result = engine.executeSql(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMN_PRIMARY_KEYS " +
            "WHERE TABLE_NAME = UPPER('" + tableName + "')"
        );
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return null;
        }
        return (String) result.rows().get(0).get("COLUMN_NAME");
    }

    public record ForeignKeyInfo(String constraintName, String column, 
                               String referencedTable, String referencedColumn) {}

    public record CheckConstraintInfo(String constraintName, String expression) {}

    public record UniqueConstraintInfo(String constraintName, String column, boolean unique) {}

    public record SqlResult(boolean success, List<String> columns, int affected, String message,
                        List<Map<String, Object>> rows, List<String> allColumns) {
        public boolean success() { return success; }
        public int affected() { return affected; }
        public String message() { return message; }
        public List<Map<String, Object>> rows() { return rows; }
    }
}