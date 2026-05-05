package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;

import static org.junify.db.storage.spi.H2StorageEngine.SqlResult;

public class ViewManager {

    private final H2StorageEngine engine;

    public ViewManager(H2StorageEngine engine) {
        this.engine = engine;
    }

    public boolean viewExists(String viewName) {
        var result = engine.executeSql(
            "SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE VIEW_NAME = UPPER('" + viewName + "')"
        );
        return result.success() && result.rows() != null && !result.rows().isEmpty();
    }

    public List<String> getViews() {
        var result = engine.executeSql(
            "SELECT VIEW_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE VIEW_SCHEMA = 'PUBLIC'"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> (String) row.get("VIEW_NAME"))
            .toList();
    }

    public String getViewDefinition(String viewName) {
        var result = engine.executeSql(
            "SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS " +
            "WHERE VIEW_NAME = UPPER('" + viewName + "')"
        );
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return null;
        }
        return (String) result.rows().get(0).get("VIEW_DEFINITION");
    }

    public SqlResult createView(String viewName, String asSelect) {
        if (viewName == null || viewName.isEmpty()) {
            return new SqlResult(false, null, 0, "View name required");
        }
        if (asSelect == null || asSelect.isEmpty()) {
            return new SqlResult(false, null, 0, "SELECT query required");
        }
        return engine.executeSql("CREATE VIEW " + viewName + " AS " + asSelect);
    }

    public SqlResult createOrReplaceView(String viewName, String asSelect) {
        if (viewExists(viewName)) {
            dropView(viewName);
        }
        return createView(viewName, asSelect);
    }

    public SqlResult dropView(String viewName) {
        return engine.executeSql("DROP VIEW " + viewName);
    }

    public SqlResult refreshView(String viewName) {
        return engine.executeSql("REFRESH VIEW " + viewName);
    }

    public List<MaterializedViewInfo> getMaterializedViews() {
        var result = engine.executeSql(
            "SELECT TABLE_NAME, ESTIMATED_SIZE FROM INFORMATION_SCHEMA.TABLES " +
            "WHERE TABLE_TYPE = 'MATERIALIZED VIEW'"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> new MaterializedViewInfo(
                (String) row.get("TABLE_NAME"),
                row.get("ESTIMATED_SIZE") != null ? 
                    ((Number) row.get("ESTIMATED_SIZE")).longValue() : 0L
            ))
            .toList();
    }

    public SqlResult createMaterializedView(String viewName, String query, long refreshIntervalMs) {
        var sql = "CREATE MATERIALIZED VIEW " + viewName + " REFRESH " + 
               (refreshIntervalMs > 0 ? "INTERVAL " + refreshIntervalMs : "ON COMMIT") + 
               " AS " + query;
        return engine.executeSql(sql);
    }

    public SqlResult refreshMaterializedView(String viewName) {
        return engine.executeSql("REFRESH MATERIALIZED VIEW " + viewName);
    }

    public record MaterializedViewInfo(String name, long estimatedSize) {}
}