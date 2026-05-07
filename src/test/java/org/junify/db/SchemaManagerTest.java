package org.junify.db;

import org.junify.db.storage.spi.H2StorageEngine;
import org.junify.db.storage.spi.SchemaManager;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class SchemaManagerTest {

    private static H2StorageEngine engine;
    private static SchemaManager schemaManager;
    private static final AtomicInteger testCounter = new AtomicInteger(0);
    
    private String uniqueTableName(String base) {
        return base + "_" + testCounter.incrementAndGet() + "_" + System.currentTimeMillis();
    }

    @BeforeAll
    static void setup() throws Exception {
        var tempDir = Files.createTempDirectory("schema-test");
        engine = new H2StorageEngine(tempDir, "testdb");
        schemaManager = engine.schemaManager();
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

    @Test
    void testCreateTable() {
        var tableName = uniqueTableName("users");
        var columns = new LinkedHashMap<String, Object>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("name", "VARCHAR(255) NOT NULL");
        columns.put("email", "VARCHAR(255)");

        var result = schemaManager.createTable(tableName, columns);

        assertTrue(result.success(), "Table creation should succeed: " + result.message());
        assertTrue(schemaManager.tableExists(tableName), "Table should exist");
    }

    @Test
    void testGetTables() {
        var tableName = uniqueTableName("test_table");
        var result = schemaManager.createTable(tableName, Map.of(
            "id", "INT PRIMARY KEY"
        ));

        var tables = schemaManager.getTables();

        assertTrue(tables != null && !tables.isEmpty(), "Should have tables");
        assertTrue(tables.stream().anyMatch(t -> t != null && t.contains("test_table")),
            "Should contain test_table (tables: " + tables + ")");
    }

    @Test
    void testGetColumns() {
        var tableName = uniqueTableName("column_test");
        var columns = new LinkedHashMap<String, Object>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("name", "VARCHAR(100) NOT NULL");

        schemaManager.createTable(tableName, columns);

        var cols = schemaManager.getColumns(tableName);

        assertFalse(cols.isEmpty(), "Should have columns (table: " + tableName + ")");
        assertTrue(cols.stream().anyMatch(c -> c != null && c.equalsIgnoreCase("id")),
            "Should have id column (columns: " + cols + ")");
        assertTrue(cols.stream().anyMatch(c -> c != null && c.equalsIgnoreCase("name")),
            "Should have name column (columns: " + cols + ")");
    }

    @Test
    void testDropTable() {
        var tableName = uniqueTableName("to_drop");
        var columns = Map.of("id", (Object) "INT PRIMARY KEY");
        schemaManager.createTable(tableName, columns);

        assertTrue(schemaManager.tableExists(tableName), "Table should exist before drop");

        var result = schemaManager.dropTable(tableName, false);

        assertTrue(result.success(), "Drop should succeed: " + result.message());
        assertFalse(schemaManager.tableExists(tableName), "Table should not exist after drop");
    }

    @Test
    void testGetTableInfo() {
        var tableName = uniqueTableName("info_test");
        var columns = Map.of(
            "id", (Object) "INT PRIMARY KEY",
            "name", "VARCHAR(100)"
        );
        schemaManager.createTable(tableName, columns);

        var info = schemaManager.getTableInfo(tableName);

        assertNotNull(info, "Table info should not be null");
        assertEquals(tableName, info.get("name"), "Table name should match");
        @SuppressWarnings("unchecked")
        var cols = (List<Map<String, Object>>) info.get("columns");
        assertFalse(cols == null || cols.isEmpty(), "Should have columns");
    }

    @Test
    void testGetColumnType() {
        var tableName = uniqueTableName("type_test");
        var columns = Map.of("id", (Object) "INT PRIMARY KEY", "data", "TEXT");
        schemaManager.createTable(tableName, columns);

        var type = schemaManager.getColumnType(tableName, "id");
        assertNotNull(type, "Column type should not be null");
        assertTrue(type.equalsIgnoreCase("INTEGER") || type.equalsIgnoreCase("INT") ||
                   type.equalsIgnoreCase("BIGINT") || type.equalsIgnoreCase("NUMERIC"),
            "Column type should be numeric (got: " + type + ")");
    }

    @Test
    void testGetSchema() {
        var result = schemaManager.getSchema();
        assertNotNull(result, "Schema result should not be null");
        assertTrue(result.success(), "Get schema should succeed: " + result.message());
    }
}
