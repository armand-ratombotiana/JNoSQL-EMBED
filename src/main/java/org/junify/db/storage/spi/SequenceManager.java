package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;

import static org.junify.db.storage.spi.H2StorageEngine.SqlResult;

public class SequenceManager {

    private final H2StorageEngine engine;

    public SequenceManager(H2StorageEngine engine) {
        this.engine = engine;
    }

    public boolean sequenceExists(String seqName) {
        var result = engine.executeSql(
            "SELECT * FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_NAME = UPPER('" + seqName + "')"
        );
        return result.success() && result.rows() != null && !result.rows().isEmpty();
    }

    public List<String> getSequences() {
        var result = engine.executeSql(
            "SELECT SEQUENCE_NAME FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = 'PUBLIC'"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> (String) row.get("SEQUENCE_NAME"))
            .toList();
    }

    public SequenceInfo getSequenceInfo(String seqName) {
        var result = engine.executeSql(
            "SELECT SEQUENCE_NAME, START_WITH, INCREMENT, MIN_VALUE, MAX_VALUE, IS_CYCLE " +
            "FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_NAME = UPPER('" + seqName + "')"
        );
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return null;
        }
        var row = result.rows().get(0);
        return new SequenceInfo(
            (String) row.get("SEQUENCE_NAME"),
            row.get("START_WITH") != null ? ((Number) row.get("START_WITH")).longValue() : 1L,
            row.get("INCREMENT") != null ? ((Number) row.get("INCREMENT")).intValue() : 1,
            row.get("MIN_VALUE") != null ? ((Number) row.get("MIN_VALUE")).longValue() : Long.MIN_VALUE,
            row.get("MAX_VALUE") != null ? ((Number) row.get("MAX_VALUE")).longValue() : Long.MAX_VALUE,
            "YES".equals(row.get("IS_CYCLE"))
        );
    }

    public SqlResult createSequence(String name) {
        return createSequence(name, 1, 1, Long.MIN_VALUE, Long.MAX_VALUE, false);
    }

    public SqlResult createSequence(String name, long start) {
        return createSequence(name, start, 1, Long.MIN_VALUE, Long.MAX_VALUE, false);
    }

    public SqlResult createSequence(String name, long start, int increment) {
        return createSequence(name, start, increment, Long.MIN_VALUE, Long.MAX_VALUE, false);
    }

    public SqlResult createSequence(String name, long start, int increment, 
                         long min, long max, boolean cycle) {
        var sql = "CREATE SEQUENCE " + name + 
                " START WITH " + start + 
                " INCREMENT BY " + increment +
                " MINVALUE " + min +
                " MAXVALUE " + max +
                (cycle ? " CYCLE" : " NO CYCLE");
        return engine.executeSql(sql);
    }

    public SqlResult alterSequenceRestart(String name, long restart) {
        return engine.executeSql("ALTER SEQUENCE " + name + " RESTART WITH " + restart);
    }

    public SqlResult alterSequenceIncrement(String name, int increment) {
        return engine.executeSql("ALTER SEQUENCE " + name + " INCREMENT BY " + increment);
    }

    public long nextValue(String sequence) {
        var result = engine.executeSql("SELECT NEXT VALUE FOR " + sequence);
        if (result.success() && result.rows() != null && !result.rows().isEmpty()) {
            return ((Number) result.rows().get(0).get("1")).longValue();
        }
        throw new RuntimeException("Failed to get next value for sequence: " + sequence);
    }

    public SqlResult dropSequence(String name) {
        return engine.executeSql("DROP SEQUENCE " + name);
    }

    public record SequenceInfo(String name, long startWith, int increment, 
                         long minValue, long maxValue, boolean cycle) {}
}