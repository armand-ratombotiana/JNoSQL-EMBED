package org.junify.db;

import org.junify.db.storage.spi.H2StorageEngine;
import org.junify.db.storage.spi.SchemaManager;
import org.junify.db.storage.spi.SqlErrorCode;
import org.junit.jupiter.api.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for enhanced H2 error messages with structured error codes.
 * Validates error classification, JSON responses, and actionable suggestions.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class EnhancedErrorHandlingTest {

    private static Path testDir;
    private static H2StorageEngine engine;
    private static SchemaManager schemaManager;

    @BeforeAll
    static void setUp() throws Exception {
        testDir = Files.createTempDirectory("h2-error-test");
        engine = new H2StorageEngine(testDir, "testdb");
        schemaManager = engine.schemaManager();

        // Create test tables (H2 uses lowercase with DATABASE_TO_LOWER=TRUE)
        Map<String, Object> usersColumns = new HashMap<>();
        usersColumns.put("id", "INT PRIMARY KEY");
        usersColumns.put("email", "VARCHAR(255) UNIQUE");
        usersColumns.put("name", "VARCHAR(100)");
        schemaManager.createTable("test_users", usersColumns);

        Map<String, Object> ordersColumns = new HashMap<>();
        ordersColumns.put("id", "INT PRIMARY KEY");
        ordersColumns.put("user_id", "INT");
        ordersColumns.put("amount", "DECIMAL(10,2)");
        schemaManager.createTable("test_orders", ordersColumns);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (engine != null) {
            engine.close();
        }
        if (testDir != null) {
            deleteDirectory(testDir.toFile());
        }
    }

    private static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }

    @Test
    @Order(1)
    @DisplayName("SYNTAX_ERROR - Invalid SQL syntax")
    void testSyntaxError() {
        // Intentional syntax error: FORM instead of FROM
        H2StorageEngine.SqlResult result = engine.executeSql("SELECT * FORM test_users");

        assertFalse(result.success(), "Should fail with syntax error");
        assertEquals("SYNTAX_ERROR", result.errorCode(), "Error code should be SYNTAX_ERROR");
        assertNotNull(result.sqlState(), "Should have SQL state");
        assertTrue(result.sqlState().startsWith("42"), "SQL state should be 42xxx for syntax errors");
        assertNotNull(result.suggestion(), "Should have suggestion");
        assertEquals("SELECT * FORM test_users", result.originalSql(), "Should preserve original SQL");

        // Verify JSON error response
        String jsonError = result.toJsonError();
        assertNotNull(jsonError, "Should generate JSON error");
        assertTrue(jsonError.contains("\"code\": \"SYNTAX_ERROR\""), "JSON should contain error code");
        assertTrue(jsonError.contains("\"sqlState\""), "JSON should contain SQL state");
        assertTrue(jsonError.contains("\"suggestion\""), "JSON should contain suggestion");
        assertTrue(jsonError.contains("\"originalSql\""), "JSON should contain original SQL");

        System.out.println("=== SYNTAX_ERROR Test ===");
        System.out.println(jsonError);
    }

    @Test
    @Order(2)
    @DisplayName("NOT_FOUND - Table does not exist")
    void testTableNotFound() {
        H2StorageEngine.SqlResult result = engine.executeSql("SELECT * FROM nonexistent_table");

        assertFalse(result.success(), "Should fail with not found error");
        assertEquals("NOT_FOUND", result.errorCode(), "Error code should be NOT_FOUND");
        assertNotNull(result.sqlState(), "Should have SQL state");
        assertNotNull(result.suggestion(), "Should have suggestion");
        assertTrue(result.suggestion().contains("Available tables"), "Suggestion should list available tables");

        System.out.println("=== NOT_FOUND Test ===");
        System.out.println(result.toJsonError());
    }

    @Test
    @Order(3)
    @DisplayName("NOT_FOUND - Column does not exist")
    void testColumnNotFound() {
        H2StorageEngine.SqlResult result = engine.executeSql("SELECT invalid_column FROM test_users");

        assertFalse(result.success(), "Should fail with not found error");
        assertEquals("NOT_FOUND", result.errorCode(), "Error code should be NOT_FOUND");

        System.out.println("=== COLUMN NOT_FOUND Test ===");
        System.out.println(result.toJsonError());
    }

    @Test
    @Order(4)
    @DisplayName("CONSTRAINT_VIOLATION - Unique constraint")
    void testUniqueConstraintViolation() {
        // Insert first user
        H2StorageEngine.SqlResult insert1 = engine.executeSql(
            "INSERT INTO test_users (id, email, name) VALUES (1, 'test@example.com', 'Test User')"
        );
        assertTrue(insert1.success(), "First insert should succeed");

        // Try to insert duplicate email
        H2StorageEngine.SqlResult insert2 = engine.executeSql(
            "INSERT INTO test_users (id, email, name) VALUES (2, 'test@example.com', 'Another User')"
        );

        assertFalse(insert2.success(), "Should fail with constraint violation");
        assertEquals("CONSTRAINT_VIOLATION", insert2.errorCode(), "Error code should be CONSTRAINT_VIOLATION");
        assertTrue(insert2.sqlState().startsWith("23"), "SQL state should start with 23 for constraint violations");
        assertNotNull(insert2.suggestion(), "Should have suggestion");

        System.out.println("=== CONSTRAINT_VIOLATION Test ===");
        System.out.println(insert2.toJsonError());
    }

    @Test
    @Order(5)
    @DisplayName("CONSTRAINT_VIOLATION - Primary key violation")
    void testPrimaryKeyViolation() {
        // Try to insert duplicate ID
        H2StorageEngine.SqlResult result = engine.executeSql(
            "INSERT INTO test_users (id, email, name) VALUES (1, 'different@example.com', 'Duplicate ID')"
        );

        assertFalse(result.success(), "Should fail with constraint violation");
        assertEquals("CONSTRAINT_VIOLATION", result.errorCode(), "Error code should be CONSTRAINT_VIOLATION");

        System.out.println("=== PRIMARY KEY VIOLATION Test ===");
        System.out.println(result.toJsonError());
    }

    @Test
    @Order(6)
    @DisplayName("DATA_EXCEPTION - Type mismatch")
    void testDataException() {
        // Try to insert string into INT column
        H2StorageEngine.SqlResult result = engine.executeSql(
            "INSERT INTO test_users (id, email, name) VALUES ('not_a_number', 'test@test.com', 'Test')"
        );

        assertFalse(result.success(), "Should fail with data exception");
        assertEquals("DATA_EXCEPTION", result.errorCode(), "Error code should be DATA_EXCEPTION");
        assertTrue(result.sqlState().startsWith("22"), "SQL state should start with 22 for data exceptions");

        System.out.println("=== DATA_EXCEPTION Test ===");
        System.out.println(result.toJsonError());
    }

    @Test
    @Order(7)
    @DisplayName("SchemaManager createTable with error codes")
    void testSchemaManagerCreateTableError() {
        Map<String, Object> columns = new HashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("name", "VARCHAR(100)");

        // Try to create table that already exists
        SchemaManager.CreateResult result = schemaManager.createTable("test_users", columns);

        assertFalse(result.success(), "Should fail - table exists");
        assertNotNull(result.errorCode(), "Should have error code");
        assertNotNull(result.message(), "Should have message");

        // Test error map conversion
        Map<String, Object> errorMap = result.toErrorMap();
        assertNotNull(errorMap, "Should generate error map");
        assertTrue(errorMap.containsKey("code"), "Error map should have code");
        assertTrue(errorMap.containsKey("message"), "Error map should have message");

        System.out.println("=== SchemaManager Create Error ===");
        System.out.println("Code: " + result.errorCode());
        System.out.println("Message: " + result.message());
    }

    @Test
    @Order(8)
    @DisplayName("SchemaManager dropTable with error codes")
    void testSchemaManagerDropTableError() {
        // Try to drop non-existent table without force
        SchemaManager.DropResult result = schemaManager.dropTable("nonexistent_table", false);

        assertFalse(result.success(), "Should fail - table doesn't exist");
        assertNotNull(result.errorCode(), "Should have error code");

        System.out.println("=== SchemaManager Drop Error ===");
        System.out.println("Code: " + result.errorCode());
        System.out.println("Message: " + result.message());
    }

    @Test
    @Order(9)
    @DisplayName("SqlErrorCode classification from SQL state")
    void testSqlErrorCodeFromSqlState() {
        assertEquals(SqlErrorCode.SYNTAX_ERROR, SqlErrorCode.fromSqlState("42000"));
        assertEquals(SqlErrorCode.SYNTAX_ERROR, SqlErrorCode.fromSqlState("42S01"));
        assertEquals(SqlErrorCode.NOT_FOUND, SqlErrorCode.fromSqlState("42S02"));
        assertEquals(SqlErrorCode.NOT_FOUND, SqlErrorCode.fromSqlState("42S22"));
        assertEquals(SqlErrorCode.PERMISSION_DENIED, SqlErrorCode.fromSqlState("42501"));
        assertEquals(SqlErrorCode.CONNECTION_ERROR, SqlErrorCode.fromSqlState("08001"));
        assertEquals(SqlErrorCode.CONNECTION_ERROR, SqlErrorCode.fromSqlState("08003"));
        assertEquals(SqlErrorCode.CONSTRAINT_VIOLATION, SqlErrorCode.fromSqlState("23000"));
        assertEquals(SqlErrorCode.TIMEOUT, SqlErrorCode.fromSqlState("57014"));
        assertEquals(SqlErrorCode.TIMEOUT, SqlErrorCode.fromSqlState("40001"));
        assertEquals(SqlErrorCode.DATA_EXCEPTION, SqlErrorCode.fromSqlState("22000"));
        assertEquals(SqlErrorCode.DATA_EXCEPTION, SqlErrorCode.fromSqlState("22003"));
        assertEquals(SqlErrorCode.INTERNAL_ERROR, SqlErrorCode.fromSqlState("HY000"));
        assertEquals(SqlErrorCode.INTERNAL_ERROR, SqlErrorCode.fromSqlState(null));
    }

    @Test
    @Order(10)
    @DisplayName("SqlErrorCode classification from message")
    void testSqlErrorCodeFromMessage() {
        assertEquals(SqlErrorCode.SYNTAX_ERROR, SqlErrorCode.fromMessage("Syntax error in SQL statement"));
        assertEquals(SqlErrorCode.SYNTAX_ERROR, SqlErrorCode.fromMessage("Unexpected token"));
        assertEquals(SqlErrorCode.NOT_FOUND, SqlErrorCode.fromMessage("Table not found"));
        assertEquals(SqlErrorCode.NOT_FOUND, SqlErrorCode.fromMessage("Column does not exist"));
        assertEquals(SqlErrorCode.CONSTRAINT_VIOLATION, SqlErrorCode.fromMessage("Unique constraint violated"));
        assertEquals(SqlErrorCode.CONSTRAINT_VIOLATION, SqlErrorCode.fromMessage("Foreign key violation"));
        assertEquals(SqlErrorCode.CONNECTION_ERROR, SqlErrorCode.fromMessage("Connection refused"));
        assertEquals(SqlErrorCode.CONNECTION_ERROR, SqlErrorCode.fromMessage("Database locked"));
        assertEquals(SqlErrorCode.TIMEOUT, SqlErrorCode.fromMessage("Query timeout exceeded"));
        assertEquals(SqlErrorCode.TIMEOUT, SqlErrorCode.fromMessage("Deadlock detected"));
        assertEquals(SqlErrorCode.PERMISSION_DENIED, SqlErrorCode.fromMessage("Access denied"));
        assertEquals(SqlErrorCode.DATA_EXCEPTION, SqlErrorCode.fromMessage("Type conversion error"));
        assertEquals(SqlErrorCode.INTERNAL_ERROR, SqlErrorCode.fromMessage("Unknown error"));
        assertEquals(SqlErrorCode.INTERNAL_ERROR, SqlErrorCode.fromMessage(null));
    }

    @Test
    @Order(11)
    @DisplayName("Successful query returns no error code")
    void testSuccessfulQuery() {
        H2StorageEngine.SqlResult result = engine.executeSql("SELECT * FROM test_users");

        assertTrue(result.success(), "Should succeed");
        assertNull(result.errorCode(), "Should not have error code on success");
        assertNull(result.toJsonError(), "Should not generate JSON error on success");
        assertNull(result.toErrorMap(), "Should not generate error map on success");
    }

    @Test
    @Order(12)
    @DisplayName("Error map structure for API responses")
    void testErrorMapStructure() {
        H2StorageEngine.SqlResult result = engine.executeSql("SELECT * FROM nonexistent");

        Map<String, Object> errorMap = result.toErrorMap();

        assertNotNull(errorMap);
        assertTrue(errorMap.containsKey("code"));
        assertTrue(errorMap.containsKey("message"));
        assertTrue(errorMap.containsKey("sqlState"));
        assertTrue(errorMap.containsKey("suggestion"));
        assertTrue(errorMap.containsKey("originalSql"));

        // Verify values
        assertEquals("NOT_FOUND", errorMap.get("code"));
        assertNotNull(errorMap.get("message"));
        assertTrue(((String) errorMap.get("sqlState")).startsWith("42"));
        assertNotNull(errorMap.get("suggestion"));
        assertEquals("SELECT * FROM nonexistent", errorMap.get("originalSql"));

        System.out.println("=== Error Map Structure ===");
        System.out.println("Code: " + errorMap.get("code"));
        System.out.println("Message: " + errorMap.get("message"));
        System.out.println("SQL State: " + errorMap.get("sqlState"));
        System.out.println("Suggestion: " + errorMap.get("suggestion"));
        System.out.println("Original SQL: " + errorMap.get("originalSql"));
    }
}
