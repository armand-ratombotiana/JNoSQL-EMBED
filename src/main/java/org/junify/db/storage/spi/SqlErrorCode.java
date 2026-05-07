package org.junify.db.storage.spi;

/**
 * Classification codes for SQL errors with actionable suggestions.
 * Maps SQL state codes to semantic error categories for API consumers.
 */
public enum SqlErrorCode {

    /**
     * SQL syntax error - malformed SQL statement.
     * SQL State: 42000 (syntax error or access rule violation)
     */
    SYNTAX_ERROR(
        "SYNTAX_ERROR",
        "SQL syntax error - check statement structure",
        "42000",
        "Review SQL syntax near the indicated position. Common issues: missing commas, incorrect keywords, unbalanced parentheses, or typos in table/column names."
    ),

    /**
     * Table or column not found.
     * SQL State: 42S02 (table not found), 42S22 (column not found)
     */
    NOT_FOUND(
        "NOT_FOUND",
        "Requested database object not found",
        "42S02",
        "Verify the table/column name exists. Use schemaManager().getTables() or schemaManager().getColumns(tableName) to list available objects."
    ),

    /**
     * Constraint violation - unique, foreign key, check, or not null.
     * SQL State: 23000 (integrity constraint violation)
     */
    CONSTRAINT_VIOLATION(
        "CONSTRAINT_VIOLATION",
        "Database constraint violation",
        "23000",
        "Check data integrity rules: UNIQUE constraint failed (duplicate value), FOREIGN KEY constraint (referenced record missing), NOT NULL constraint (null in required column), or CHECK constraint (value fails validation)."
    ),

    /**
     * Database connection error.
     * SQL State: 08000 (connection exception)
     */
    CONNECTION_ERROR(
        "CONNECTION_ERROR",
        "Database connection failed",
        "08000",
        "Verify database is running and connection parameters are correct. Check network connectivity, credentials, and that the database file is not locked by another process."
    ),

    /**
     * Query timeout or deadlock.
     * SQL State: 57014 (query canceled), 40001 (serialization failure)
     */
    TIMEOUT(
        "TIMEOUT",
        "Query execution timeout or deadlock",
        "57014",
        "Query took too long or was deadlocked. Optimize the query with indexes, reduce result set size, increase query timeout with setQueryTimeout(seconds), or retry the transaction."
    ),

    /**
     * Permission denied - insufficient privileges.
     * SQL State: 42501 (insufficient privilege)
     */
    PERMISSION_DENIED(
        "PERMISSION_DENIED",
        "Insufficient privileges for operation",
        "42501",
        "Current user lacks permission for this operation. Contact database administrator to grant required privileges or use a different user account."
    ),

    /**
     * Data conversion or type mismatch error.
     * SQL State: 22000 (data exception)
     */
    DATA_EXCEPTION(
        "DATA_EXCEPTION",
        "Data type conversion or validation error",
        "22000",
        "Check data types match column definitions. Common issues: string too long for VARCHAR, invalid date format, numeric overflow, or null in non-nullable column."
    ),

    /**
     * Internal database error - unexpected condition.
     * SQL State: HY000 (general error)
     */
    INTERNAL_ERROR(
        "INTERNAL_ERROR",
        "Internal database error",
        "HY000",
        "Unexpected database error. Check database logs for details. If problem persists, verify database integrity and consider restoring from backup."
    );

    private final String code;
    private final String description;
    private final String sqlState;
    private final String suggestion;

    SqlErrorCode(String code, String description, String sqlState, String suggestion) {
        this.code = code;
        this.description = description;
        this.sqlState = sqlState;
        this.suggestion = suggestion;
    }

    /**
     * Get the semantic error code for API responses.
     */
    public String getCode() {
        return code;
    }

    /**
     * Get human-readable description of the error type.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Get the standard SQL state code for this error category.
     */
    public String getSqlState() {
        return sqlState;
    }

    /**
     * Get actionable suggestion for fixing this error type.
     */
    public String getSuggestion() {
        return suggestion;
    }

    /**
     * Classify an SQL state code into semantic error category.
     *
     * @param sqlState the SQL state code from SQLException
     * @return the corresponding SqlErrorCode
     */
    public static SqlErrorCode fromSqlState(String sqlState) {
        if (sqlState == null) {
            return INTERNAL_ERROR;
        }

        String state = sqlState.toUpperCase();

        // Syntax errors: 42xxx
        if (state.startsWith("42")) {
            if (state.equals("42501")) {
                return PERMISSION_DENIED;
            }
            if (state.equals("42S02") || state.equals("42S22")) {
                return NOT_FOUND;
            }
            return SYNTAX_ERROR;
        }

        // Connection errors: 08xxx
        if (state.startsWith("08")) {
            return CONNECTION_ERROR;
        }

        // Constraint violations: 23xxx
        if (state.startsWith("23")) {
            return CONSTRAINT_VIOLATION;
        }

        // Timeouts/deadlocks: 57xxx, 40xxx
        if (state.startsWith("57") || state.startsWith("40")) {
            return TIMEOUT;
        }

        // Data exceptions: 22xxx
        if (state.startsWith("22")) {
            return DATA_EXCEPTION;
        }

        // Default to internal error
        return INTERNAL_ERROR;
    }

    /**
     * Classify from SQLException message patterns when SQL state is unavailable.
     *
     * @param message the SQLException message
     * @return the corresponding SqlErrorCode
     */
    public static SqlErrorCode fromMessage(String message) {
        if (message == null) {
            return INTERNAL_ERROR;
        }

        String msg = message.toLowerCase();

        if (msg.contains("syntax") || msg.contains("parse error") ||
            msg.contains("unexpected token") || msg.contains("expected")) {
            return SYNTAX_ERROR;
        }

        if (msg.contains("not found") || msg.contains("does not exist") ||
            msg.contains("unknown column") || msg.contains("table doesn't exist")) {
            return NOT_FOUND;
        }

        if (msg.contains("constraint") || msg.contains("duplicate") ||
            msg.contains("integrity") || msg.contains("foreign key") ||
            msg.contains("unique") || msg.contains("not null")) {
            return CONSTRAINT_VIOLATION;
        }

        if (msg.contains("connection") || msg.contains("network") ||
            msg.contains("io exception") || msg.contains("locked")) {
            return CONNECTION_ERROR;
        }

        if (msg.contains("timeout") || msg.contains("deadlock") ||
            msg.contains("canceled") || msg.contains("long")) {
            return TIMEOUT;
        }

        if (msg.contains("permission") || msg.contains("privilege") ||
            msg.contains("access denied") || msg.contains("unauthorized")) {
            return PERMISSION_DENIED;
        }

        if (msg.contains("conversion") || msg.contains("type") ||
            msg.contains("cast") || msg.contains("overflow") ||
            msg.contains("truncation")) {
            return DATA_EXCEPTION;
        }

        return INTERNAL_ERROR;
    }
}
