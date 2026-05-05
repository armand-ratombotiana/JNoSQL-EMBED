package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;

import static org.junify.db.storage.spi.H2StorageEngine.SqlResult;

public class StoredProcedureManager {

    private final H2StorageEngine engine;

    public StoredProcedureManager(H2StorageEngine engine) {
        this.engine = engine;
    }

    public boolean procedureExists(String procName) {
        var result = engine.executeSql(
            "SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = UPPER('" + procName + "')"
        );
        return result.success() && result.rows() != null && !result.rows().isEmpty();
    }

    public List<String> getProcedures() {
        var result = engine.executeSql(
            "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'PUBLIC'"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> (String) row.get("ROUTINE_NAME"))
            .toList();
    }

    public ProcedureInfo getProcedureInfo(String procName) {
        var result = engine.executeSql(
            "SELECT ROUTINE_NAME, ROUTINE_TYPE, DATA_TYPE, SPECIFIC_NAME " +
            "FROM INFORMATION_SCHEMA.PARAMS WHERE PARAMETER_NAME IS NOT NULL " +
            "AND ROUTINE_NAME = UPPER('" + procName + "')"
        );
        if (!result.success() || result.rows() == null) {
            return null;
        }
        
        var params = new ArrayList<ProcedureParam>();
        for (var row : result.rows()) {
            var mode = row.get("PARAMETER_MODE");
            params.add(new ProcedureParam(
                (String) row.get("PARAMETER_NAME"),
                (String) row.get("DATA_TYPE"),
                mode != null && "IN".equalsIgnoreCase(mode.toString())
            ));
        }
        
        return new ProcedureInfo(procName, params);
    }

    public SqlResult createProcedure(String name, String as) {
        return engine.executeSql(
            "CREATE PROCEDURE " + name + " AS " + as
        );
    }

    public SqlResult createFunction(String name, String returns, String as) {
        return engine.executeSql(
            "CREATE FUNCTION " + name + " RETURNS " + returns + " AS " + as
        );
    }

    public SqlResult dropProcedure(String name) {
        return engine.executeSql("DROP PROCEDURE " + name);
    }

    public SqlResult dropFunction(String name) {
        return engine.executeSql("DROP FUNCTION " + name);
    }

    public record ProcedureInfo(String name, List<ProcedureParam> params) {}
    public record ProcedureParam(String name, String dataType, boolean isInput) {}
}