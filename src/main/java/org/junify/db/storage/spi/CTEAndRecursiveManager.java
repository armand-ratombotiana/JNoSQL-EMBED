package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;

public class CTEAndRecursiveManager {

    private final H2StorageEngine engine;

    public CTEAndRecursiveManager(H2StorageEngine engine) {
        this.engine = engine;
    }

    public SqlResult withClause(String withDef, String select) {
        var sql = "WITH " + withDef + " " + select;
        return engine.executeSql(sql);
    }

    public SqlResult withMultipleCTEs(String cteDefinitions, String mainQuery) {
        return engine.executeSql("WITH " + cteDefinitions + " " + mainQuery);
    }

    public SqlResult recursiveCTE(String cteName, String initialQuery, String recursiveQuery, String baseCondition) {
        var sql = "WITH RECURSIVE " + cteName + " AS (" + initialQuery + " UNION ALL " + 
                 recursiveQuery + ") " +
                 "SELECT * FROM " + cteName + " WHERE " + baseCondition;
        return engine.executeSql(sql);
    }

    public SqlResult hierarchicalQuery(String table, String startWith, String connectBy) {
        return engine.executeSql(
            "SELECT * FROM " + table + " START WITH " + startWith + " CONNECT BY " + connectBy
        );
    }

    public SqlResult hierarchicalWithLevel(String table, String startWith, String connectBy) {
        return engine.executeSql(
            "SELECT *, LEVEL FROM " + table + " START WITH " + startWith + 
            " CONNECT BY " + connectBy + " ORDER SIBLINGS BY 1"
        );
    }

    public SqlResult parentChildRecursive(String table, String idCol, String parentCol, 
                                     String rootCondition) {
        var cte = "WITH RECURSIVE org_chart AS (" +
                  "SELECT " + idCol + ", " + parentCol + ", 1 AS level " +
                  "FROM " + table + " " +
                  "WHERE " + parentCol + " IS NULL " +
                  "UNION ALL " +
                  "SELECT e." + idCol + ", e." + parentCol + ", p.level + 1 " +
                  "FROM " + table + " e " +
                  "JOIN org_chart p ON e." + parentCol + " = p." + idCol + ") " +
                  "SELECT * FROM org_chart WHERE " + rootCondition;
        return engine.executeSql(cte);
    }

    public SqlResult graphTraversal(String nodeTable, String edgesTable, 
                              String startNode, int maxDepth) {
        var cte = "WITH RECURSIVE path_finder AS (" +
                  "SELECT start_node, start_node AS path, 1 AS depth " +
                  "FROM " + edgesTable + " " +
                  "WHERE start_node = '" + startNode + "' " +
                  "UNION ALL " +
                  "SELECT e.end_node, p.path || ' -> ' || e.end_node, p.depth + 1 " +
                  "FROM " + edgesTable + " e " +
                  "JOIN path_finder p ON e.start_node = p.path " +
                  "WHERE p.depth < " + maxDepth + ") " +
                  "SELECT * FROM path_finder";
        return engine.executeSql(cte);
    }

    public SqlResult fibonacciRecursive(int limit) {
        var sql = "WITH RECURSIVE fib AS (" +
                 "SELECT 0 AS n, 0 AS fib_val, 1 AS next_val " +
                 "UNION ALL " +
                 "SELECT n + 1, next_val, fib_val + next_val " +
                 "FROM fib WHERE n < " + limit + ") " +
                 "SELECT fib_val FROM fib";
        return engine.executeSql(sql);
    }

    public SqlResult findInInterval(String table, String column, int minVal, int maxVal) {
        var sql = "WITH RECURSIVE intervals AS (" +
                 "SELECT " + column + ", " + column + " AS next_val " +
                 "FROM " + table + " " +
                 "WHERE " + column + " >= " + minVal + " " +
                 "UNION ALL " +
                 "SELECT next_val, next_val + 1 " +
                 "FROM intervals " +
                 "WHERE next_val < " + maxVal + ") " +
                 "SELECT * FROM intervals";
        return engine.executeSql(sql);
    }

    public static class SqlResult {
        public List<Object[]> rows = new ArrayList<>();
        public List<String> columns = new ArrayList<>();
        
        public SqlResult() {}
        
        public SqlResult(List<String> cols, List<Object[]> rows) {
            this.columns = cols;
            this.rows = rows;
        }
        
        public List<Object[]> getRows() { return rows; }
        public List<String> getColumns() { return columns; }
        public int rowCount() { return rows.size(); }
    }
}