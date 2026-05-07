package org.junify.db;

import org.junify.db.storage.spi.H2StorageEngine;
import org.junify.db.storage.spi.SchemaManager;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for H2 prepared statements implementation.
 * Verifies SQL injection prevention, parameter binding, and statement caching.
 */
@Tag("integration")
public class PreparedStatementTest {

    private static H2StorageEngine engine;
    private static SchemaManager schemaManager;
    private static String testTableName;

    @BeforeAll
    static void setup() throws Exception {
        Path tempDir = Files.createTempDirectory("prepared-statement-test");
        engine = new H2StorageEngine(tempDir, "testdb");
        schemaManager = engine.schemaManager();
        
        // Create a test table for all tests
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("name", "VARCHAR(255) NOT NULL");
        columns.put("email", "VARCHAR(255)");
        columns.put("age", "INT");
        columns.put("created_at", "TIMESTAMP");
        testTableName = "test_users_" + System.currentTimeMillis();
        var result = schemaManager.createTable(testTableName, columns);
        assertTrue(result.success(), "Test table creation should succeed: " + result.message());
    }

    @AfterAll
    static void teardown() {
        if (engine != null) {
            try {
                engine.close();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    // ==================== SQL INJECTION PREVENTION TESTS ====================

    @Test
    void testPreventSQLInjectionInWhereClause() {
        int testId = 1001;
        
        // Insert test data
        var insertResult = engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            testId, "John Doe", "john@example.com", 30, Timestamp.from(Instant.now())
        );
        assertTrue(insertResult.success(), "Insert should succeed: " + insertResult.message());

        // Malicious input attempting SQL injection
        String maliciousEmail = "' OR '1'='1";

        // Should NOT return all rows - prepared statement escapes the input
        var result = engine.executeSql(
            "SELECT * FROM " + testTableName + " WHERE email = ?",
            maliciousEmail
        );

        assertTrue(result.success(), "Query should succeed: " + result.message());
        assertEquals(0, result.rows().size(), "Should return 0 rows (injection prevented)");
    }

    @Test
    void testPreventSQLInjectionInTableName() {
        // Attempt to inject via table name parameter
        String maliciousTable = "test_users; DROP TABLE test_users; --";

        // Using prepared statement with table name as parameter
        var result = engine.executeSql(
            "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
            maliciousTable.toLowerCase()
        );

        assertTrue(result.success(), "Query should succeed");
        assertEquals(0, result.rows().size(), "Should not match malicious table name");

        // Verify table still exists
        assertTrue(schemaManager.tableExists(testTableName), "Table should still exist after injection attempt");
    }

    @Test
    void testPreventSQLInjectionWithSpecialCharacters() {
        int testId = 1002;
        String specialName = "O'Reilly; DROP TABLE test_users;--";

        var insertResult = engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            testId, specialName, "oreilly@example.com", 25, Timestamp.from(Instant.now())
        );

        assertTrue(insertResult.success(), "Insert should succeed with special characters: " + insertResult.message());

        // Verify we can retrieve it safely
        var selectResult = engine.executeSql(
            "SELECT name FROM " + testTableName + " WHERE id = ?",
            testId
        );

        assertTrue(selectResult.success(), "Select should succeed: " + selectResult.message());
        assertEquals(1, selectResult.rows().size(), "Should return 1 row");
        assertEquals(specialName, selectResult.rows().get(0).get("NAME"), "Name should match exactly");
    }

    // ==================== PARAMETER BINDING TESTS ====================

    @Test
    void testStringParameterBinding() {
        int testId = 1010;
        
        var insertResult = engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            testId, "Alice", "alice@example.com", 28, Timestamp.from(Instant.now())
        );
        assertTrue(insertResult.success(), "Insert should succeed: " + insertResult.message());

        var result = engine.executeSql(
            "SELECT name, email FROM " + testTableName + " WHERE id = ?",
            testId
        );

        assertTrue(result.success(), "Select should succeed: " + result.message());
        assertEquals(1, result.rows().size());
        assertEquals("Alice", result.rows().get(0).get("NAME"));
        assertEquals("alice@example.com", result.rows().get(0).get("EMAIL"));
    }

    @Test
    void testIntegerParameterBinding() {
        int testId = 1020;
        
        var insertResult = engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            testId, "Bob", "bob@example.com", 35, Timestamp.from(Instant.now())
        );
        assertTrue(insertResult.success(), "Insert should succeed: " + insertResult.message());

        var result = engine.executeSql(
            "SELECT name FROM " + testTableName + " WHERE id = ?",
            testId
        );

        assertTrue(result.success(), "Select should succeed: " + result.message());
        assertEquals(1, result.rows().size(), "Should return exactly 1 row");
        assertEquals("Bob", result.rows().get(0).get("NAME"));
    }

    @Test
    void testMultipleParameterTypes() {
        int testId = 1030;
        Timestamp now = Timestamp.from(Instant.now());
        Timestamp future = Timestamp.from(Instant.now().plusSeconds(1000));

        var insertResult = engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            testId, "Charlie", "charlie@example.com", 40, now
        );
        assertTrue(insertResult.success(), "Insert should succeed: " + insertResult.message());

        var result = engine.executeSql(
            "SELECT * FROM " + testTableName + " WHERE age > ? AND created_at < ?",
            30, future
        );

        assertTrue(result.success(), "Select should succeed: " + result.message());
        assertEquals(1, result.rows().size());
        assertEquals("Charlie", result.rows().get(0).get("NAME"));
    }

    @Test
    void testNullParameterBinding() {
        int testId = 1040;
        
        var insertResult = engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            testId, "Diana", null, 45, Timestamp.from(Instant.now())
        );
        assertTrue(insertResult.success(), "Insert should succeed: " + insertResult.message());

        var result = engine.executeSql(
            "SELECT name FROM " + testTableName + " WHERE email IS NULL"
        );

        assertTrue(result.success(), "Select should succeed: " + result.message());
        assertTrue(result.rows().size() >= 1, "Should have at least 1 row with null email");
    }

    @Test
    void testUpdateWithParameters() {
        int testId = 1050;
        
        var insertResult = engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            testId, "Eve", "eve@example.com", 25, Timestamp.from(Instant.now())
        );
        assertTrue(insertResult.success(), "Insert should succeed: " + insertResult.message());

        var updateResult = engine.executeSql(
            "UPDATE " + testTableName + " SET age = ?, email = ? WHERE id = ?",
            26, "eve.updated@example.com", testId
        );

        assertTrue(updateResult.success(), "Update should succeed: " + updateResult.message());
        assertEquals(1, updateResult.affected());

        var verifyResult = engine.executeSql(
            "SELECT age, email FROM " + testTableName + " WHERE id = ?",
            testId
        );

        assertTrue(verifyResult.success(), "Verify select should succeed: " + verifyResult.message());
        assertEquals(1, verifyResult.rows().size());
        assertEquals(26, verifyResult.rows().get(0).get("AGE"));
        assertEquals("eve.updated@example.com", verifyResult.rows().get(0).get("EMAIL"));
    }

    @Test
    void testDeleteWithParameters() {
        int testId = 1060;
        
        var insertResult = engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            testId, "Frank", "frank@example.com", 30, Timestamp.from(Instant.now())
        );
        assertTrue(insertResult.success(), "Insert should succeed: " + insertResult.message());

        var deleteResult = engine.executeSql(
            "DELETE FROM " + testTableName + " WHERE id = ?",
            testId
        );

        assertTrue(deleteResult.success(), "Delete should succeed: " + deleteResult.message());
        assertEquals(1, deleteResult.affected());

        var verifyResult = engine.executeSql(
            "SELECT * FROM " + testTableName + " WHERE id = ?",
            testId
        );

        assertTrue(verifyResult.success(), "Verify select should succeed: " + verifyResult.message());
        assertEquals(0, verifyResult.rows().size());
    }

    // ==================== STATEMENT CACHING TESTS ====================

    @Test
    void testStatementCaching() {
        String sql = "SELECT * FROM " + testTableName + " WHERE id = ?";

        // Execute same query multiple times
        for (int i = 0; i < 5; i++) {
            var result = engine.executeSql(sql, 100 + i);
            assertTrue(result.success(), "Query " + i + " should succeed: " + result.message());
        }

        // Verify cache has entries
        Map<String, Object> stats = engine.stats();
        Integer cacheSize = (Integer) stats.get("statementCacheSize");
        assertTrue(cacheSize != null && cacheSize > 0, "Statement cache should have entries");
    }

    @Test
    void testClearStatementCache() {
        String sql = "SELECT * FROM " + testTableName + " WHERE age = ?";

        // Execute to populate cache
        engine.executeSql(sql, 25);

        Map<String, Object> statsBefore = engine.stats();
        Integer cacheSizeBefore = (Integer) statsBefore.get("statementCacheSize");

        // Clear cache
        engine.clearStatementCache();

        Map<String, Object> statsAfter = engine.stats();
        Integer cacheSizeAfter = (Integer) statsAfter.get("statementCacheSize");

        assertEquals(0, cacheSizeAfter, "Cache should be empty after clear");
    }

    // ==================== SCHEMA MANAGER TESTS ====================

    @Test
    void testSchemaManagerTableExistsWithPreparedStatements() {
        // Test tableExists uses prepared statements
        assertTrue(schemaManager.tableExists(testTableName), testTableName + " table should exist");
        assertFalse(schemaManager.tableExists("nonexistent_table"), "Non-existent table should not exist");
    }

    @Test
    void testSchemaManagerGetColumnsWithPreparedStatements() {
        List<String> cols = schemaManager.getColumns(testTableName);

        assertFalse(cols.isEmpty(), "Should have columns");
        assertTrue(cols.stream().anyMatch(c -> c != null && c.equalsIgnoreCase("id")), "Should have id column (cols: " + cols + ")");
        assertTrue(cols.stream().anyMatch(c -> c != null && c.equalsIgnoreCase("name")), "Should have name column");
    }

    @Test
    void testSchemaManagerGetColumnTypeWithPreparedStatements() {
        String idType = schemaManager.getColumnType(testTableName, "id");
        String dataType = schemaManager.getColumnType(testTableName, "name");

        assertNotNull(idType, "ID type should not be null");
        assertNotNull(dataType, "Name type should not be null");
        assertTrue(dataType.toUpperCase().contains("VARCHAR") || dataType.toUpperCase().contains("CHAR"), 
            "Name column should be VARCHAR (got: " + dataType + ")");
    }

    // ==================== EDGE CASES ====================

    @Test
    void testEmptyParameterArray() {
        var result = engine.executeSql("SELECT * FROM " + testTableName, new Object[0]);
        assertTrue(result.success(), "Query with empty params should succeed: " + result.message());
    }

    @Test
    void testBooleanParameterBinding() {
        // Create a table with boolean column
        String boolTable = "bool_test_" + System.currentTimeMillis();
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("active", "BOOLEAN");
        var createResult = schemaManager.createTable(boolTable, columns);
        assertTrue(createResult.success(), "Bool table creation should succeed: " + createResult.message());

        var insert1 = engine.executeSql(
            "INSERT INTO " + boolTable + " (id, active) VALUES (?, ?)",
            1, true
        );
        assertTrue(insert1.success(), "Insert true should succeed: " + insert1.message());
        
        var insert2 = engine.executeSql(
            "INSERT INTO " + boolTable + " (id, active) VALUES (?, ?)",
            2, false
        );
        assertTrue(insert2.success(), "Insert false should succeed: " + insert2.message());

        // Query using 1/0 for boolean (H2 compatibility)
        var trueResult = engine.executeSql(
            "SELECT id FROM " + boolTable + " WHERE active = ?",
            1
        );

        assertTrue(trueResult.success(), "Select should succeed: " + trueResult.message());
        assertTrue(trueResult.rows().size() >= 1, "Should have at least 1 row with active=true (got: " + trueResult.rows().size() + ")");
    }

    @Test
    void testLikeParameterBinding() {
        int testId = 1070;
        
        var insertResult = engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            testId, "Test User", "test@example.com", 30, Timestamp.from(Instant.now())
        );
        assertTrue(insertResult.success(), "Insert should succeed: " + insertResult.message());

        var result = engine.executeSql(
            "SELECT name FROM " + testTableName + " WHERE name LIKE ?",
            "%Test%"
        );

        assertTrue(result.success(), "Select should succeed: " + result.message());
        assertEquals(1, result.rows().size());
        assertEquals("Test User", result.rows().get(0).get("NAME"));
    }

    @Test
    void testInClauseWithMultipleParameters() {
        int baseId = 1080;
        
        engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            baseId, "User1", "user1@example.com", 25, Timestamp.from(Instant.now())
        );
        engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            baseId + 1, "User2", "user2@example.com", 30, Timestamp.from(Instant.now())
        );
        engine.executeSql(
            "INSERT INTO " + testTableName + " (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            baseId + 2, "User3", "user3@example.com", 35, Timestamp.from(Instant.now())
        );

        var result = engine.executeSql(
            "SELECT name FROM " + testTableName + " WHERE id IN (?, ?, ?)",
            baseId, baseId + 1, baseId + 2
        );

        assertTrue(result.success(), "Select should succeed: " + result.message());
        assertEquals(3, result.rows().size());
    }
}
