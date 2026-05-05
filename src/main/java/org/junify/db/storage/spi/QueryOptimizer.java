package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class QueryOptimizer {

    private final H2StorageEngine engine;

    public QueryOptimizer(H2StorageEngine engine) {
        this.engine = engine;
    }

    public QueryPlan explain(String sql) {
        var explainSql = "EXPLAIN " + sql;
        var result = engine.executeSql(explainSql);
        
        if (!result.success()) {
            return new QueryPlan(sql, false, Collections.emptyList(), result.message());
        }
        
        var planInfo = new ArrayList<String>();
        var executionType = "UNKNOWN";
        
        if (result.rows() != null) {
            for (var row : result.rows()) {
                for (var entry : row.entrySet()) {
                    var value = entry.getValue();
                    if (value != null) {
                        planInfo.add(entry.getKey() + ": " + value);
                    }
                }
            }
        }
        
        if (sql.trim().toUpperCase().startsWith("SELECT")) {
            executionType = "READ";
        } else if (sql.trim().toUpperCase().startsWith("INSERT")) {
            executionType = "WRITE";
        } else if (sql.trim().toUpperCase().startsWith("UPDATE")) {
            executionType = "WRITE";
        } else if (sql.trim().toUpperCase().startsWith("DELETE")) {
            executionType = "WRITE";
        }
        
        return new QueryPlan(sql, true, planInfo, executionType);
    }

    public List<String> getQueryPlan(String sql) {
        var plan = explain(sql);
        return plan.plan();
    }

    public boolean isOptimized(String sql) {
        var upperSql = sql.toUpperCase();
        
        if (upperSql.contains("SELECT") && upperSql.contains("WHERE")) {
            if (!upperSql.contains("INDEX") && !upperSql.contains("FORCE INDEX")) {
                return false;
            }
        }
        
        if (upperSql.contains("LIKE")) {
            if (upperSql.contains("LIKE '%")) {
                return false;
            }
        }
        
        if (upperSql.contains("OR") && !upperSql.contains("UNION")) {
            return false;
        }
        
        return true;
    }

    public SqlResult analyzeQuery(String sql) {
        var plan = explain(sql);
        
        return new SqlResult(true, 
            List.of("sql", "success", "optimized", "executionType", "plan"),
            plan.plan().size(),
            "Query analyzed: " + plan.executionType(),
            plan.plan().stream()
                .map(row -> Map.of(
                    "sql", sql,
                    "success", plan.success(),
                    "optimized", isOptimized(sql),
                    "executionType", plan.executionType(),
                    "plan", plan.plan()
                ))
            .toList(),
            List.of("sql", "success", "optimized", "executionType", "plan")
        );
    }

    public SqlResult suggestIndexes(String tableName, int threshold) {
        var suggestions = new ArrayList<String>();
        
        var colResult = engine.executeSql(
            "SELECT COLUMN_NAME, COLUMN_POSITION " +
            "FROM INFORMATION_SCHEMA.COLUMN_USAGE WHERE TABLE_NAME = UPPER('" + tableName + "') " +
            "ORDER BY COLUMN_POSITION"
        );
        
        if (colResult.success() && colResult.rows() != null) {
            var whereCols = new StringJoiner(", ");
            for (var row : colResult.rows()) {
                var col = (String) row.get("COLUMN_NAME");
                suggestions.add("CREATE INDEX idx_" + tableName + "_" + col + 
                           " ON " + tableName + " (" + col + ")");
            }
        }
        
        return new SqlResult(true, List.of("suggestion"), suggestions.size(), 
            "Consider creating indexes for WHERE clauses: " + suggestions,
            suggestions.stream()
                .map(s -> Map.of("suggestion", (Object)s))
                .collect(Collectors.toList()),
            List.of("suggestion"));
    }

    public SqlResult tableStats(String tableName) {
        var stats = new HashMap<String, Object>();
        
        var size = engine.executeSql(
            "SELECT SUM(ESTIMATED_SIZE) FROM INFORMATION_SCHEMA.TABLES " +
            "WHERE TABLE_NAME = UPPER('" + tableName + "')"
        );
        
        var rowCount = engine.executeSql("SELECT COUNT(*) FROM " + tableName);
        if (rowCount.success()) {
            stats.put("rowCount", rowCount.affected());
        }
        
        var sample = engine.executeSql(
            "SELECT * FROM " + tableName + " LIMIT 1"
        );
        if (sample.success() && sample.columns() != null) {
            stats.put("columnCount", sample.columns().size());
        }
        
        return new SqlResult(true, List.of("metric", "value"), stats.size(),
            "Table stats for " + tableName,
            stats.entrySet().stream()
                .map(e -> Map.of("metric", e.getKey(), "value", e.getValue()))
                .toList(),
            List.of("metric", "value"));
    }

    public record QueryPlan(String sql, boolean success, List<String> plan, String executionType) {}
    
    public static class SqlResult {
        private final boolean success;
        private final java.util.List<String> columns;
        private final int affected;
        private final String message;
        private final java.util.List<java.util.Map<String, Object>> rows;
        private final java.util.List<String> allColumns;

        public SqlResult(boolean success, java.util.List<String> columns, int affected, 
                   String message, java.util.List<java.util.Map<String, Object>> rows, 
                   java.util.List<String> allColumns) {
            this.success = success;
            this.columns = columns;
            this.affected = affected;
            this.message = message;
            this.rows = rows;
            this.allColumns = allColumns;
        }

        public boolean success() { return success; }
        public java.util.List<String> columns() { return columns; }
        public int affected() { return affected; }
        public String message() { return message; }
        public java.util.List<java.util.Map<String, Object>> rows() { return rows; }
    }
}