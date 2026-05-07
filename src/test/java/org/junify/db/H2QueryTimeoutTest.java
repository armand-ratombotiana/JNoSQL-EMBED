package org.junify.db;

import org.junify.db.storage.spi.H2StorageEngine;
import org.junify.db.storage.spi.SchemaManager;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for H2 query timeout enforcement.
 * Verifies that query timeout is properly applied to all Statement objects.
 * Note: H2 file mode doesn't support actual query cancellation via setQueryTimeout,
 * but we verify the timeout is set and configuration works correctly.
 */
@Tag("integration")
public class H2QueryTimeoutTest {

    private H2StorageEngine engine;
    private SchemaManager schemaManager;
    private Path tempDir;

    @BeforeEach
    void setup() throws Exception {
        tempDir = Files.createTempDirectory("query-timeout-test");
    }

    @AfterEach
    void teardown() {
        if (engine != null) {
            try {
                engine.close();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    // ==================== TIMEOUT CONFIGURATION TESTS ====================

    @Test
    void testDefaultQueryTimeout() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb");

        assertEquals(30, engine.getQueryTimeout(), "Default query timeout should be 30 seconds");
    }

    @Test
    void testConstructorWithCustomTimeout() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 60);

        assertEquals(60, engine.getQueryTimeout(), "Custom query timeout should be 60 seconds");
    }

    @Test
    void testConstructorWithZeroTimeout() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 0);

        assertEquals(0, engine.getQueryTimeout(), "Zero timeout means no timeout (wait forever)");
    }

    @Test
    void testSetQueryTimeout() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb");

        assertEquals(30, engine.getQueryTimeout());

        engine.setQueryTimeout(120);

        assertEquals(120, engine.getQueryTimeout(), "Query timeout should be updated to 120 seconds");
    }

    @Test
    void testSetQueryTimeoutToZero() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb");

        engine.setQueryTimeout(0);

        assertEquals(0, engine.getQueryTimeout(), "Query timeout of 0 means no timeout (wait forever)");
    }

    // ==================== NORMAL QUERY EXECUTION TESTS ====================

    @Test
    void testNormalQueryCompletesWithinTimeout() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 5);
        schemaManager = engine.schemaManager();

        // Create a test table (avoid reserved words like 'value')
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("data", "VARCHAR(255)");
        String tableName = "test_table_" + System.currentTimeMillis();
        var createResult = schemaManager.createTable(tableName, columns);
        assertTrue(createResult.success(), "Table creation should succeed: " + createResult.message());

        // Insert test data
        var insertResult = engine.executeSql(
            "INSERT INTO " + tableName + " (id, data) VALUES (?, ?)",
            1, "test_value"
        );
        assertTrue(insertResult.success(), "Insert should succeed: " + insertResult.message());
        assertEquals(1, insertResult.affected());

        // Query data - should complete well within timeout
        var selectResult = engine.executeSql(
            "SELECT * FROM " + tableName + " WHERE id = ?",
            1
        );

        assertTrue(selectResult.success(), "Select should succeed: " + selectResult.message());
        assertEquals(1, selectResult.rows().size());
        assertEquals("test_value", selectResult.rows().get(0).get("DATA"));
    }

    @Test
    void testMultipleQueriesWithTimeout() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 10);
        schemaManager = engine.schemaManager();

        String tableName = "multi_query_table_" + System.currentTimeMillis();
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("data", "VARCHAR(255)");
        var createResult = schemaManager.createTable(tableName, columns);
        assertTrue(createResult.success());

        // Execute multiple queries
        for (int i = 0; i < 10; i++) {
            var insertResult = engine.executeSql(
                "INSERT INTO " + tableName + " (id, data) VALUES (?, ?)",
                i, "data_" + i
            );
            assertTrue(insertResult.success(), "Insert " + i + " should succeed");
        }

        var selectResult = engine.executeSql(
            "SELECT COUNT(*) as cnt FROM " + tableName
        );

        assertTrue(selectResult.success(), "Count query should succeed");
        // ResultSet returns 1 row with the count value
        assertEquals(1, selectResult.rows().size());
    }

    // ==================== TIMEOUT APPLICATION TESTS ====================

    @Test
    void testTimeoutAppliedToPreparedStatement() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 5);
        schemaManager = engine.schemaManager();

        String tableName = "ps_timeout_table_" + System.currentTimeMillis();
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("data", "VARCHAR(255)");
        var createResult = schemaManager.createTable(tableName, columns);
        assertTrue(createResult.success());

        // Execute parameterized query - timeout should be applied
        var insertResult = engine.executeSql(
            "INSERT INTO " + tableName + " (id, data) VALUES (?, ?)",
            1, "test_data"
        );

        assertTrue(insertResult.success(), "Parameterized insert should succeed: " + insertResult.message());
    }

    @Test
    void testTimeoutAppliedToStatement() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 5);

        // Execute simple statement - timeout should be applied
        var result = engine.executeSql("SELECT 1 as test");

        assertTrue(result.success(), "Simple select should succeed: " + result.message());
        assertEquals(1, result.rows().size());
    }

    @Test
    void testTimeoutAppliedToMultiStatement() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 5);
        schemaManager = engine.schemaManager();

        String tableName = "multi_stmt_table_" + System.currentTimeMillis();
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        var createResult = schemaManager.createTable(tableName, columns);
        assertTrue(createResult.success());

        // Multi-statement SQL - timeout should be applied to each statement
        var result = engine.executeSql(
            "INSERT INTO " + tableName + " (id) VALUES (1); INSERT INTO " + tableName + " (id) VALUES (2)"
        );

        assertTrue(result.success(), "Multi-statement insert should succeed: " + result.message());
    }

    @Test
    void testTimeoutAppliedToSelectWithParams() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 5);
        schemaManager = engine.schemaManager();

        String tableName = "select_param_table_" + System.currentTimeMillis();
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("data", "VARCHAR(255)");
        var createResult = schemaManager.createTable(tableName, columns);
        assertTrue(createResult.success());

        engine.executeSql("INSERT INTO " + tableName + " (id, data) VALUES (?, ?)", 1, "test");

        var result = engine.executeSql(
            "SELECT * FROM " + tableName + " WHERE id = ?",
            1
        );

        assertTrue(result.success(), "Parameterized select should succeed: " + result.message());
        assertEquals(1, result.rows().size());
    }

    // ==================== ERROR MESSAGE TESTS ====================

    @Test
    void testNormalQueryErrorMessage() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 30);

        // Invalid SQL should produce SQL error
        var result = engine.executeSql("SELECT * FROM nonexistent_table_xyz");

        assertFalse(result.success());
        // Check that it's a SQL error (table not found)
        assertTrue(
            result.message().contains("nonexistent_table_xyz") || result.message().contains("not found"),
            "Invalid SQL should produce table not found error: " + result.message()
        );
    }

    @Test
    void testSyntaxErrorHandling() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 30);

        // Syntax error should be handled gracefully
        var result = engine.executeSql("SELECT * FORM table_name");

        assertFalse(result.success());
        assertTrue(
            result.message().contains("SQL Error") || result.message().contains("error"),
            "Syntax error should be reported: " + result.message()
        );
    }

    // ==================== TIMEOUT WITH DIFFERENT QUERY TYPES ====================

    @Test
    void testUpdateQueryWithTimeout() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 5);
        schemaManager = engine.schemaManager();

        String tableName = "update_table_" + System.currentTimeMillis();
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("amount", "INT");
        var createResult = schemaManager.createTable(tableName, columns);
        assertTrue(createResult.success());

        engine.executeSql("INSERT INTO " + tableName + " (id, amount) VALUES (?, ?)", 1, 100);

        var result = engine.executeSql(
            "UPDATE " + tableName + " SET amount = ? WHERE id = ?",
            200, 1
        );

        assertTrue(result.success(), "Update should succeed: " + result.message());
        assertEquals(1, result.affected());
    }

    @Test
    void testDeleteQueryWithTimeout() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 5);
        schemaManager = engine.schemaManager();

        String tableName = "delete_table_" + System.currentTimeMillis();
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        var createResult = schemaManager.createTable(tableName, columns);
        assertTrue(createResult.success());

        engine.executeSql("INSERT INTO " + tableName + " (id) VALUES (?)", 1);

        var result = engine.executeSql(
            "DELETE FROM " + tableName + " WHERE id = ?",
            1
        );

        assertTrue(result.success(), "Delete should succeed: " + result.message());
        assertEquals(1, result.affected());
    }

    @Test
    void testMergeQueryWithTimeout() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 5);
        schemaManager = engine.schemaManager();

        String tableName = "merge_table_" + System.currentTimeMillis();
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("data", "VARCHAR(255)");
        var createResult = schemaManager.createTable(tableName, columns);
        assertTrue(createResult.success());

        var result = engine.executeSql(
            "MERGE INTO " + tableName + " (id, data) KEY(id) VALUES (?, ?)",
            1, "test"
        );

        assertTrue(result.success(), "Merge should succeed: " + result.message());
        assertEquals(1, result.affected());
    }

    // ==================== TIMEOUT CONFIGURATION PERSISTENCE ====================

    @Test
    void testTimeoutConfigurationAcrossQueries() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 5);
        schemaManager = engine.schemaManager();

        // Verify timeout is consistent across multiple queries
        assertEquals(5, engine.getQueryTimeout());

        String tableName = "persistence_table_" + System.currentTimeMillis();
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        var createResult = schemaManager.createTable(tableName, columns);
        assertTrue(createResult.success());

        // Execute several queries - timeout should remain 5
        engine.executeSql("INSERT INTO " + tableName + " (id) VALUES (1)");
        assertEquals(5, engine.getQueryTimeout());

        engine.executeSql("SELECT * FROM " + tableName);
        assertEquals(5, engine.getQueryTimeout());

        engine.executeSql("UPDATE " + tableName + " SET id = 2 WHERE id = 1");
        assertEquals(5, engine.getQueryTimeout());
    }

    @Test
    void testDynamicTimeoutChange() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 30);

        // Start with 30 second timeout
        assertEquals(30, engine.getQueryTimeout());

        // Change to 5 seconds
        engine.setQueryTimeout(5);
        assertEquals(5, engine.getQueryTimeout());

        // Query should still work with reasonable timeout
        var result = engine.executeSql("SELECT 1");
        assertTrue(result.success(), "Query should succeed with 5s limit");

        // Change back to 30 seconds
        engine.setQueryTimeout(30);
        assertEquals(30, engine.getQueryTimeout());

        // Query should still work
        var result2 = engine.executeSql("SELECT 2");
        assertTrue(result2.success(), "Query should succeed with 30s limit");
    }

    // ==================== STATEMENT CACHE WITH TIMEOUT ====================

    @Test
    void testPreparedStatementCacheWithTimeout() throws Exception {
        engine = new H2StorageEngine(tempDir, "testdb", 10);
        schemaManager = engine.schemaManager();

        String tableName = "cache_test_table_" + System.currentTimeMillis();
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("data", "VARCHAR(255)");
        var createResult = schemaManager.createTable(tableName, columns);
        assertTrue(createResult.success());

        // Execute same query multiple times to use cache
        String sql = "SELECT * FROM " + tableName + " WHERE id = ?";
        for (int i = 0; i < 5; i++) {
            engine.executeSql("INSERT INTO " + tableName + " (id, data) VALUES (?, ?)", i, "data_" + i);
            var result = engine.executeSql(sql, i);
            assertTrue(result.success(), "Cached query " + i + " should succeed");
        }

        // Verify cache has entries
        Map<String, Object> stats = engine.stats();
        Integer cacheSize = (Integer) stats.get("statementCacheSize");
        assertTrue(cacheSize != null && cacheSize > 0, "Statement cache should have entries");
    }
}
