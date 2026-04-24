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

    public SqlResult recursive CTE(String cteName, String initialQuery, String recursiveQuery, String baseCondition) {
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
        var cte = """
            RECURSIVE org_chart AS (
                SELECT """ + idCol + """, """ + parentCol + """, 1 AS level
                FROM """ + table + """
                WHERE """ + parentCol + """ IS NULL
                
                UNION ALL
                
                SELECT e.""" + idCol + """, e.""" + parentCol + """, p.level + 1
                FROM """ + table + """ e
                JOIN org_chart p ON e.""" + parentCol + """ = p.""" + idCol + """
            )
            SELECT * FROM org_chart WHERE """ + rootCondition;
        return engine.executeSql(cte);
    }

    public SqlResult graphTraversal(String nodeTable, String edgesTable, 
                                   String startNode, String maxDepth) {
        var cte = """
            RECURSIVE path_finder AS (
                SELECT start_node, start_node AS path, 1 AS depth
                FROM """ + edgesTable + """
                WHERE start_node = '""" + startNode + """'
                
                UNION ALL
                
                SELECT e.end_node, p.path || ' -> ' || e.end_node, p.depth + 1
                FROM """ + edgesTable + """ e
                JOIN path_finder p ON e.start_node = p.path
                WHERE p.depth < """ + maxDepth + """
            )
            SELECT * FROM path_finder""";
        return engine.executeSql(cte);
    }

    public SqlResult fibonacciRecursive(int limit) {
        var cte = """
            RECURSIVE fib(a, b, n) AS (
                SELECT 0, 1, 1
                UNION ALL
                SELECT b, a + b, n + 1
                FROM fib WHERE n < """ + limit + """
            )
            SELECT a FROM fib""";
        return engine.executeSql(cte);
    }

    public SqlResult generateSeries(int start, int end) {
        var cte = """
            RECURSIVE nums(n) AS (
                SELECT """ + start + """
                UNION ALL
                SELECT n + 1 FROM nums WHERE n < """ + end + """
            )
            SELECT n FROM nums""";
        return engine.executeSql(cte);
    }

    public SqlResult dateRangeRecursive(String startDate, int days) {
        var cte = """
            RECURSIVE dates(d) AS (
                SELECT CAST('""" + startDate + """' AS DATE)
                UNION ALL
                SELECT DATE_ADD(d, 1) FROM dates WHERE d < DATE_ADD(CAST('""" + startDate + """' AS DATE), """ + days + """ DAYS)
            )
            SELECT * FROM dates""";
        return engine.executeSql(cte);
    }

    public SqlResult pagingWithCTE(String table, int pageSize, int pageNumber) {
        var offset = pageSize * (pageNumber - 1);
        var cte = """
            WITH paged AS (
                SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rn, * FROM """ + table + """
            )
            SELECT * FROM paged WHERE rn > """ + offset + """ AND rn <= """ + (offset + pageSize);
        return engine.executeSql(cte);
    }

    public SqlResult runningTotalsWithCTE(String table, String valueCol, String orderCol) {
        return engine.executeSql(
            "WITH cumulative AS (SELECT *, SUM(" + valueCol + ") OVER (ORDER BY " + orderCol + ") AS running_total FROM " + table + ") SELECT * FROM cumulative"
        );
    }

    public SqlResult leadLagComparison(String table, String column, String orderBy) {
        return engine.executeSql(
            "SELECT *, LAG(" + column + ") OVER (ORDER BY " + orderBy + ") AS prev_val, " +
            "LEAD(" + column + ") OVER (ORDER BY " + orderBy + ") AS next_val FROM " + table
        );
    }

    public SqlResult percentileCont(double percentile) {
        return engine.executeSql(
            "SELECT PERCENTILE_CONT(" + percentile + ") WITHIN GROUP (ORDER BY val) OVER() AS p" + (int)(percentile * 100) + " FROM (VALUES (10), (20), (30), (40), (50)) AS t(val)"
        );
    }

    public SqlResult median() {
        return engine.executeSql(
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) OVER() AS median FROM (VALUES (10), (20), (30), (40), (50)) AS t(val)"
        );
    }

    public record SqlResult(boolean success, List<String> columns, int affected, String message,
                        List<Map<String, Object>> rows, List<String> allColumns) {
        public boolean success() { return success; }
    }
}