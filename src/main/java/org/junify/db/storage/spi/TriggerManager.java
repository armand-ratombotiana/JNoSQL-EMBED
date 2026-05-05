package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;

import static org.junify.db.storage.spi.H2StorageEngine.SqlResult;

public class TriggerManager {

    private final H2StorageEngine engine;

    public TriggerManager(H2StorageEngine engine) {
        this.engine = engine;
    }

    public boolean triggerExists(String triggerName) {
        var result = engine.executeSql(
            "SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_NAME = UPPER('" + triggerName + "')"
        );
        return result.success() && result.rows() != null && !result.rows().isEmpty();
    }

    public List<String> getTriggers() {
        var result = engine.executeSql(
            "SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> (String) row.get("TRIGGER_NAME"))
            .toList();
    }

    public List<String> getTriggersForTable(String tableName) {
        var result = engine.executeSql(
            "SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS " +
            "WHERE TABLE_NAME = UPPER('" + tableName + "')"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> (String) row.get("TRIGGER_NAME"))
            .toList();
    }

    public TriggerInfo getTriggerInfo(String triggerName) {
        var result = engine.executeSql(
            "SELECT TRIGGER_NAME, TRIGGER_TYPE, TABLE_NAME, TRIGGER_DEFINITION " +
            "FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_NAME = UPPER('" + triggerName + "')"
        );
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return null;
        }
        var row = result.rows().get(0);
        return new TriggerInfo(
            (String) row.get("TRIGGER_NAME"),
            (String) row.get("TRIGGER_TYPE"),
            (String) row.get("TABLE_NAME"),
            (String) row.get("TRIGGER_DEFINITION")
        );
    }

    public SqlResult createTrigger(String name, TriggerEvent event, String tableName, String as) {
        var sql = "CREATE TRIGGER " + name + " " + 
                event.sql() + " ON " + tableName + " " + as;
        return engine.executeSql(sql);
    }

    public SqlResult createBeforeInsertTrigger(String name, String tableName, String as) {
        return createTrigger(name, TriggerEvent.BEFORE_INSERT, tableName, as);
    }

    public SqlResult createAfterInsertTrigger(String name, String tableName, String as) {
        return createTrigger(name, TriggerEvent.AFTER_INSERT, tableName, as);
    }

    public SqlResult createBeforeUpdateTrigger(String name, String tableName, String as) {
        return createTrigger(name, TriggerEvent.BEFORE_UPDATE, tableName, as);
    }

    public SqlResult createAfterUpdateTrigger(String name, String tableName, String as) {
        return createTrigger(name, TriggerEvent.AFTER_UPDATE, tableName, as);
    }

    public SqlResult createBeforeDeleteTrigger(String name, String tableName, String as) {
        return createTrigger(name, TriggerEvent.BEFORE_DELETE, tableName, as);
    }

    public SqlResult createAfterDeleteTrigger(String name, String tableName, String as) {
        return createTrigger(name, TriggerEvent.AFTER_DELETE, tableName, as);
    }

    public SqlResult dropTrigger(String triggerName) {
        return engine.executeSql("DROP TRIGGER " + triggerName);
    }

    public SqlResult enableTrigger(String triggerName) {
        return engine.executeSql("ALTER TRIGGER " + triggerName + " ENABLE");
    }

    public SqlResult disableTrigger(String triggerName) {
        return engine.executeSql("ALTER TRIGGER " + triggerName + " DISABLE");
    }

    public enum TriggerEvent {
        BEFORE_INSERT("BEFORE INSERT"),
        AFTER_INSERT("AFTER INSERT"),
        BEFORE_UPDATE("BEFORE UPDATE"),
        AFTER_UPDATE("AFTER UPDATE"),
        BEFORE_DELETE("BEFORE DELETE"),
        AFTER_DELETE("AFTER DELETE");

        private final String sql;

        TriggerEvent(String sql) {
            this.sql = sql;
        }

        public String sql() {
            return sql;
        }
    }

    public record TriggerInfo(String name, String type, String tableName, String definition) {}
}