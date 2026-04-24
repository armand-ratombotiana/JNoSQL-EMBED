package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;

public class WindowFunctionManager {

    private final H2StorageEngine engine;

    public WindowFunctionManager(H2StorageEngine engine) {
        this.engine = engine;
    }

    public SqlResult rowNumber(String table, String partitionBy, String orderBy) {
        return engine.executeSql(
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY " + partitionBy + 
            " ORDER BY " + orderBy + ") AS row_num FROM " + table
        );
    }

    public SqlResult rank(String table, String partitionBy, String orderBy) {
        return engine.executeSql(
            "SELECT *, RANK() OVER (PARTITION BY " + partitionBy + 
            " ORDER BY " + orderBy + ") AS rank_num FROM " + table
        );
    }

    public SqlResult denseRank(String table, String partitionBy, String orderBy) {
        return engine.executeSql(
            "SELECT *, DENSE_RANK() OVER (PARTITION BY " + partitionBy + 
            " ORDER BY " + orderBy + ") AS dense_rank FROM " + table
        );
    }

    public SqlResult runningTotal(String table, String valueColumn, String orderBy) {
        return engine.executeSql(
            "SELECT *, SUM(" + valueColumn + ") OVER (ORDER BY " + orderBy + 
            ") AS running_total FROM " + table
        );
    }

    public SqlResult movingAverage(String table, String valueColumn, String orderBy, int window) {
        return engine.executeSql(
            "SELECT *, AVG(" + valueColumn + ") OVER (ORDER BY " + orderBy + 
            " ROWS BETWEEN " + (window - 1) + " PRECEDING AND CURRENT ROW) AS moving_avg FROM " + table
        );
    }

    public SqlResult lag(String table, String column, String orderBy, int offset) {
        return engine.executeSql(
            "SELECT *, LAG(" + column + ", " + offset + ") OVER (ORDER BY " + orderBy + ") AS lag_value FROM " + table
        );
    }

    public SqlResult lead(String table, String column, String orderBy, int offset) {
        return engine.executeSql(
            "SELECT *, LEAD(" + column + ", " + offset + ") OVER (ORDER BY " + orderBy + ") AS lead_value FROM " + table
        );
    }

    public SqlResult firstValue(String table, String column, String orderBy) {
        return engine.executeSql(
            "SELECT *, FIRST_VALUE(" + column + ") OVER (ORDER BY " + orderBy + ") AS first_val FROM " + table
        );
    }

    public SqlResult lastValue(String table, String column, String orderBy) {
        return engine.executeSql(
            "SELECT *, LAST_VALUE(" + column + ") OVER (ORDER BY " + orderBy + ") AS last_val FROM " + table
        );
    }

    public SqlResult ntile(String table, String column, int buckets) {
        return engine.executeSql(
            "SELECT *, NTILE(" + buckets + ") OVER (ORDER BY " + column + ") AS ntile_group FROM " + table
        );
    }

    public SqlResult cumeDist(String table, String column, String orderBy) {
        return engine.executeSql(
            "SELECT *, CUME_DIST() OVER (ORDER BY " + orderBy + ") AS cume_dist FROM " + table
        );
    }

    public SqlResult percentRank(String table, String column, String orderBy) {
        return engine.executeSql(
            "SELECT *, PERCENT_RANK() OVER (ORDER BY " + orderBy + ") AS percent_rank FROM " + table
        );
    }

    public SqlResult nthValue(String table, String column, int n, String orderBy) {
        return engine.executeSql(
            "SELECT *, NTH_VALUE(" + column + ", " + n + ") OVER (ORDER BY " + orderBy + ") AS nth_val FROM " + table
        );
    }

    public SqlResult overPartition(String table, String partitionBy, String aggFunc, String column, String orderBy) {
        return engine.executeSql(
            "SELECT *, " + aggFunc + "(" + column + ") OVER (PARTITION BY " + partitionBy + 
            " ORDER BY " + orderBy + ") AS result FROM " + table
        );
    }

    public SqlResult frameRange(String table, String column, String orderBy, String frameSpec) {
        return engine.executeSql(
            "SELECT *, SUM(" + column + ") OVER (ORDER BY " + orderBy + " " + frameSpec + ") AS frame_sum FROM " + table
        );
    }

    public SqlResult frameRows(String table, String column, String orderBy, int preceding, int following) {
        return engine.executeSql(
            "SELECT *, SUM(" + column + ") OVER (ORDER BY " + orderBy + 
            " ROWS BETWEEN " + preceding + " PRECEDING AND " + following + " FOLLOWING) AS window_sum FROM " + table
        );
    }

    public SqlResult explainAnalyze(String sql) {
        return engine.executeSql("EXPLAIN ANALYZE " + sql);
    }

    public record SqlResult(boolean success, List<String> columns, int affected, String message,
                        List<Map<String, Object>> rows, List<String> allColumns) {
        public boolean success() { return success; }
    }
}