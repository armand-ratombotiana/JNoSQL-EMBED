package org.junify.db.storage.spi;

import java.util.HashMap;
import java.util.Map;

/**
 * Structured SQL exception with classified error code and actionable suggestions.
 * Provides programmatic error handling for API consumers.
 */
public class SqlException extends RuntimeException {

    private final SqlErrorCode errorCode;
    private final String sqlState;
    private final String suggestion;
    private final String originalSql;
    private final Map<String, Object> context;

    /**
     * Create a SqlException from an SQLException with classification.
     *
     * @param message the error message
     * @param errorCode the classified error code
     * @param sqlState the SQL state code from the database
     * @param suggestion actionable suggestion for fixing the error
     * @param originalSql the SQL statement that caused the error
     * @param cause the underlying SQLException
     */
    public SqlException(String message, SqlErrorCode errorCode, String sqlState,
                        String suggestion, String originalSql, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.sqlState = sqlState;
        this.suggestion = suggestion;
        this.originalSql = originalSql;
        this.context = new HashMap<>();
    }

    /**
     * Create a SqlException from an SQLException with auto-classification.
     *
     * @param message the error message
     * @param sqlState the SQL state code from the database
     * @param originalSql the SQL statement that caused the error
     * @param cause the underlying SQLException
     */
    public SqlException(String message, String sqlState, String originalSql, Throwable cause) {
        this(message,
             SqlErrorCode.fromSqlState(sqlState),
             sqlState,
             SqlErrorCode.fromSqlState(sqlState).getSuggestion(),
             originalSql,
             cause);
    }

    /**
     * Create a SqlException with minimal information.
     *
     * @param errorCode the error code
     * @param message the error message
     * @param originalSql the SQL statement that caused the error
     */
    public SqlException(SqlErrorCode errorCode, String message, String originalSql) {
        this(message, errorCode, errorCode.getSqlState(),
             errorCode.getSuggestion(), originalSql, null);
    }

    /**
     * Get the classified error code for programmatic handling.
     *
     * @return the SqlErrorCode enum value
     */
    public SqlErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * Get the SQL state code from the database.
     *
     * @return the SQL state code (e.g., "42000", "23000")
     */
    public String getSqlState() {
        return sqlState;
    }

    /**
     * Get actionable suggestion for fixing this error.
     *
     * @return human-readable suggestion
     */
    public String getSuggestion() {
        return suggestion;
    }

    /**
     * Get the original SQL statement that caused the error.
     *
     * @return the SQL statement
     */
    public String getOriginalSql() {
        return originalSql;
    }

    /**
     * Get additional context information attached to this exception.
     *
     * @return context map
     */
    public Map<String, Object> getContext() {
        return context;
    }

    /**
     * Add context information to this exception.
     *
     * @param key the context key
     * @param value the context value
     * @return this exception for chaining
     */
    public SqlException withContext(String key, Object value) {
        this.context.put(key, value);
        return this;
    }

    /**
     * Convert to a structured error map for JSON serialization.
     *
     * @return map with error details suitable for JSON response
     */
    public Map<String, Object> toErrorMap() {
        Map<String, Object> error = new HashMap<>();
        error.put("code", errorCode.getCode());
        error.put("message", getMessage());
        error.put("sqlState", sqlState);
        error.put("suggestion", suggestion);
        if (originalSql != null && !originalSql.isEmpty()) {
            error.put("originalSql", originalSql);
        }
        if (!context.isEmpty()) {
            error.put("context", context);
        }
        return error;
    }

    /**
     * Convert to JSON error response format.
     *
     * @return JSON-formatted error response
     */
    public String toJson() {
        StringBuilder json = new StringBuilder("{\n  \"error\": {\n");
        json.append("    \"code\": \"").append(escapeJson(errorCode.getCode())).append("\",\n");
        json.append("    \"message\": \"").append(escapeJson(getMessage())).append("\",\n");
        json.append("    \"sqlState\": \"").append(escapeJson(sqlState)).append("\",\n");
        json.append("    \"suggestion\": \"").append(escapeJson(suggestion)).append("\"");
        if (originalSql != null && !originalSql.isEmpty()) {
            json.append(",\n    \"originalSql\": \"").append(escapeJson(originalSql)).append("\"");
        }
        if (!context.isEmpty()) {
            json.append(",\n    \"context\": ").append(mapToJson(context));
        }
        json.append("\n  }\n}");
        return json.toString();
    }

    /**
     * Escape special characters for JSON string.
     */
    private String escapeJson(String value) {
        if (value == null) return "";
        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }

    /**
     * Convert a map to JSON string (simple implementation for context).
     */
    private String mapToJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
            Object value = entry.getValue();
            if (value instanceof String) {
                sb.append("\"").append(escapeJson(value.toString())).append("\"");
            } else if (value instanceof Number || value instanceof Boolean) {
                sb.append(value.toString());
            } else {
                sb.append("\"").append(escapeJson(String.valueOf(value))).append("\"");
            }
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toString() {
        return String.format("SqlException[%s] %s (SQL State: %s, Suggestion: %s)",
                            errorCode.getCode(), getMessage(), sqlState, suggestion);
    }
}
