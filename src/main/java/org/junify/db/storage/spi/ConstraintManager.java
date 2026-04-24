package org.junify.db.storage.spi;

import java.util.List;
import java.util.Map;

public class ConstraintManager {
    
    public ConstraintManager(H2StorageEngine engine) {}
    
    public List<Map<String, Object>> getAllConstraints(String tableName) {
        return List.of();
    }
    
    public SqlResult addCheckConstraint(String tableName, String constraintName, String condition) {
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public SqlResult addForeignKey(String tableName, String constraintName, String refTable, String col, String refCol) {
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public SqlResult addUniqueConstraint(String tableName, String constraintName, String... columns) {
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public SqlResult dropConstraint(String constraintName) {
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public record SqlResult(boolean success, List<String> columns, int affected, String message, 
        List<Map<String, Object>> rows, List<String> allColumns) {}
}