package org.junify.db;

import org.junify.db.storage.spi.H2StorageEngine;
import org.junify.db.storage.spi.SchemaManager;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class SchemaManagerTest {

    private static H2StorageEngine engine;
    private static SchemaManager schemaManager;

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
        var columns = new LinkedHashMap<String, Object>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("name", "VARCHAR(255) NOT NULL");
        columns.put("email", "VARCHAR(255)");

        var result = schemaManager.createTable("users", columns);

        assertTrue(result.success(), "Table creation should succeed: " + result.message());
        assertTrue(schemaManager.tableExists("users"), "Table should exist");
    }

    @Test
    void testGetTables() {
        var result = schemaManager.createTable("test_table", Map.of(
            "id", "INT PRIMARY KEY"
        ));

        var tables = schemaManager.getTables();

        assertFalse(tables.isEmpty(), "Should have tables");
        assertTrue(tables.stream().anyMatch(t -> t.equalsIgnoreCase("test_table")),
            "Should contain test_table (tables: " + tables + ")");
    }

    @Test
    void testGetColumns() {
        var columns = new LinkedHashMap<String, Object>();
        columns.put("id", "INT PRIMARY KEY");
        columns.put("name", "VARCHAR(100) NOT NULL");

        schemaManager.createTable("column_test", columns);

        var cols = schemaManager.getColumns("column_test");

        assertFalse(cols.isEmpty(), "Should have columns");
        assertTrue(cols.stream().anyMatch(c -> c.equalsIgnoreCase("id")),
            "Should have id column (columns: " + cols + ")");
        assertTrue(cols.stream().anyMatch(c -> c.equalsIgnoreCase("name")),
            "Should have name column (columns: " + cols + ")");
    }

    @Test
    void testDropTable() {
        var columns = Map.of("id", (Object) "INT PRIMARY KEY");
        schemaManager.createTable("to_drop", columns);

        assertTrue(schemaManager.tableExists("to_drop"), "Table should exist before drop");

        var result = schemaManager.dropTable("to_drop", false);

        assertTrue(result.success(), "Drop should succeed: " + result.message());
        assertFalse(schemaManager.tableExists("to_drop"), "Table should not exist after drop");
    }

    @Test
    void testGetTableInfo() {
        var columns = Map.of(
            "id", (Object) "INT PRIMARY KEY",
            "name", "VARCHAR(100)"
        );
        schemaManager.createTable("info_test", columns);

        var info = schemaManager.getTableInfo("info_test");

        assertNotNull(info, "Table info should not be null");
        assertEquals("info_test", info.get("name"), "Table name should match");
        @SuppressWarnings("unchecked")
        var cols = (List<Map<String, Object>>) info.get("columns");
        assertFalse(cols == null || cols.isEmpty(), "Should have columns");
    }

    @Test
    void testGetColumnType() {
        var columns = Map.of("id", (Object) "INT PRIMARY KEY", "data", "TEXT");
        schemaManager.createTable("type_test", columns);

        var type = schemaManager.getColumnType("type_test", "id");
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
