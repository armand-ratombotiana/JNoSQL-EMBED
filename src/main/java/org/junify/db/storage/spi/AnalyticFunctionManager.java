package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;

public class AnalyticFunctionManager {

    private final H2StorageEngine engine;

    public AnalyticFunctionManager(H2StorageEngine engine) {
        this.engine = engine;
    }

    public SqlResult cumulativeDistribution(String table, String column) {
        return engine.executeSql(
            "SELECT *, CUME_DIST() OVER (ORDER BY " + column + ") AS cumedist FROM " + table
        );
    }

    public SqlResult percentRank(String table, String column) {
        return engine.executeSql(
            "SELECT *, PERCENT_RANK() OVER (ORDER BY " + column + ") AS pk FROM " + table
        );
    }

    public SqlResult ntile(String table, int n) {
        return engine.executeSql(
            "SELECT *, NTILE(" + n + ") OVER (ORDER BY 1) AS quartile FROM " + table
        );
    }

    public SqlResult firstLastValue(String table, String column, String orderBy) {
        return engine.executeSql(
            "SELECT *, FIRST_VALUE(" + column + ") OVER (ORDER BY " + orderBy + ") AS first_val, " +
            "LAST_VALUE(" + column + ") OVER (ORDER BY " + orderBy + ") AS last_val FROM " + table
        );
    }

    public SqlResult nthValue(String table, String column, int n, String orderBy) {
        return engine.executeSql(
            "SELECT *, NTH_VALUE(" + column + ", " + n + ") OVER (ORDER BY " + orderBy + ") AS nth FROM " + table
        );
    }

    public SqlResult ratioToReport(String table, String column) {
        return engine.executeSql(
            "SELECT *, " + column + "/SUM(" + column + ") OVER() AS ratio_to_report FROM " + table
        );
    }

    public SqlResult ratioBetween(String table, String column, String partition, String orderBy) {
        return engine.executeSql(
            "SELECT *, " + column + "/SUM(" + column + ") OVER (PARTITION BY " + partition + 
            " ORDER BY " + orderBy + " ROWS UNBOUNDED PRECEDING) AS ratio_to_part FROM " + table
        );
    }

    public SqlResult keepDenseFirst(String table, String filterCol, String keepCol, String orderBy) {
        return engine.executeSql(
            "SELECT " + filterCol + ", MIN(" + keepCol + ") KEEP (DENSE_RANK FIRST ORDER BY " + orderBy + ") OVER (PARTITION BY " + filterCol + ") AS kept FROM " + table + " GROUP BY " + filterCol
        );
    }

    public SqlResult boolAnd(String table, String column, String partition) {
        return engine.executeSql(
            "SELECT " + partition + ", BOOL_AND(" + column + ") OVER (PARTITION BY " + partition + ") AS result FROM " + table
        );
    }

    public SqlResult boolOr(String table, String column, String partition) {
        return engine.executeSql(
            "SELECT " + partition + ", BOOL_OR(" + column + ") OVER (PARTITION BY " + partition + ") AS result FROM " + table
        );
    }

    public SqlResult listAgg(String table, String column, String separator, String partition) {
        return engine.executeSql(
            "SELECT " + partition + ", STRING_AGG(" + column + ", '" + separator + "') OVER (PARTITION BY " + partition + ") AS aggregated FROM " + table
        );
    }

    public SqlResult statisticalFunctions(String table, String valueCol, String byCol) {
        var sql = "SELECT " + byCol + ", " +
            "COUNT(*) OVER (PARTITION BY " + byCol + ") AS count, " +
            "SUM(" + valueCol + ") OVER (PARTITION BY " + byCol + ") AS sum, " +
            "AVG(" + valueCol + ") OVER (PARTITION BY " + byCol + ") AS avg, " +
            "MIN(" + valueCol + ") OVER (PARTITION BY " + byCol + ") AS min, " +
            "MAX(" + valueCol + ") OVER (PARTITION BY " + byCol + ") AS max, " +
            "VAR_POP(" + valueCol + ") OVER (PARTITION BY " + byCol + ") AS variance, " +
            "STDDEV_POP(" + valueCol + ") OVER (PARTITION BY " + byCol + ") AS stddev " +
            "FROM " + table + " GROUP BY " + byCol;
        return engine.executeSql(sql);
    }

    public SqlResult median(String table, String valueCol, String partition) {
        return engine.executeSql(
            "SELECT " + partition + ", PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY " + valueCol + 
            ") OVER (PARTITION BY " + partition + ") AS median FROM " + table + " GROUP BY " + partition
        );
    }

    public SqlResult mode(String table, String valueCol, String groupBy) {
        return engine.executeSql(
            "SELECT " + groupBy + ", " + valueCol + " AS mode_value, COUNT(*) AS freq " +
            "FROM " + table + " GROUP BY " + groupBy + ", " + valueCol + " " +
            "HAVING COUNT(*) = (SELECT MAX(cnt) FROM (SELECT COUNT(*) AS cnt FROM " + 
            table + " GROUP BY " + groupBy + ", " + valueCol + ") t)"
        );
    }

    public SqlResult runningMinMax(String table, String valueCol, String orderBy) {
        return engine.executeSql(
            "SELECT *, MIN(" + valueCol + ") OVER (ORDER BY " + orderBy + ") AS running_min, " +
            "MAX(" + valueCol + ") OVER (ORDER BY " + orderBy + ") AS running_max FROM " + table
        );
    }

    public SqlResult exponentialMovingAverage(String table, String valueCol, double alpha, String orderBy) {
        return engine.executeSql(
            "SELECT *, AVG(" + valueCol + ") OVER (ORDER BY " + orderBy + " ROWS BETWEEN " +
            "UNBOUNDED PRECEDING AND 1 PRECEDING) AS ema FROM " + table
        );
    }

    public SqlResult variance(String table, String valueCol, String partition) {
        return engine.executeSql(
            "SELECT " + partition + ", VAR_POP(" + valueCol + ") AS var_pop, VAR_SAMP(" + valueCol + 
            ") AS var_samp, STDDEV_POP(" + valueCol + ") AS stddev_pop, STDDEV_SAMP(" + valueCol + 
            ") AS stddev_samp FROM " + table + " GROUP BY " + partition
        );
    }

    public SqlResult correlation(String table, String xCol, String yCol, String partition) {
        return engine.executeSql(
            "SELECT " + partition + ", CORR(" + xCol + ", " + yCol + ") OVER (PARTITION BY " + partition + ") AS correlation FROM " + table + " GROUP BY " + partition
        );
    }

    public SqlResult covariance(String table, String xCol, String yCol, String partition) {
        return engine.executeSql(
            "SELECT " + partition + ", COVAR_POP(" + xCol + ", " + yCol + ") AS covar_pop, COVAR_SAMP(" + xCol + ", " + yCol + ") AS covar_samp FROM " + table + " GROUP BY " + partition
        );
    }

    public SqlResult regrLinear(String table, String xCol, String yCol, String partition) {
        return engine.executeSql(
            "SELECT " + partition + ", REGR_SLOPE(" + yCol + ", " + xCol + ") AS slope, REGR_INTERCEPT(" + yCol + ", " + xCol + ") AS intercept, REGR_R2(" + yCol + ", " + xCol + ") AS r_squared FROM " + table + " GROUP BY " + partition
        );
    }

    public record SqlResult(boolean success, List<String> columns, int affected, String message,
                        List<Map<String, Object>> rows, List<String> allColumns) {
        public boolean success() { return success; }
    }
}