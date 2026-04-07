package org.jnosql.embed.column;

import org.jnosql.embed.storage.StorageEngine;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ColumnFamily {

    private final String name;
    private final StorageEngine engine;

    public ColumnFamily(String name, StorageEngine engine) {
        this.name = name;
        this.engine = engine;
    }

    public String name() {
        return name;
    }

    public void put(String rowKey, String column, Object value) {
        var row = getRow(rowKey);
        row.put(column, value);
        saveRow(rowKey, row);
    }

    public Object get(String rowKey, String column) {
        var row = getRow(rowKey);
        return row.get(column);
    }

    public Map<String, Object> getRow(String rowKey) {
        var json = engine.get(name, rowKey);
        if (json == null) return new LinkedHashMap<>();
        return org.jnosql.embed.util.JsonSerde.fromJson(json, Map.class);
    }

    public void deleteColumn(String rowKey, String column) {
        var row = getRow(rowKey);
        row.remove(column);
        if (row.isEmpty()) {
            engine.delete(name, rowKey);
        } else {
            saveRow(rowKey, row);
        }
    }

    public void deleteRow(String rowKey) {
        engine.delete(name, rowKey);
    }

    public Set<String> getRowKeys() {
        return new HashSet<>(engine.keys(name));
    }

    public long countRows() {
        return engine.keys(name).size();
    }

    private void saveRow(String rowKey, Map<String, Object> row) {
        engine.put(name, rowKey, org.jnosql.embed.util.JsonSerde.toJson(row));
    }
}
