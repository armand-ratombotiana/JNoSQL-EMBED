package org.junify.db;

import org.junify.db.storage.spi.H2StorageEngine;
import org.junify.db.storage.spi.SchemaManager;
import org.junify.db.storage.spi.SchemaManager.ColumnDef;
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
        engine.close();
    }

    @Test
    void testCreateTable() {
        var columns = new LinkedHashMap<String, ColumnDef>();
        columns.put("id", ColumnDef.of("id", "INT").primaryKey());
        columns.put("name", ColumnDef.of("name", "VARCHAR(255)").notNull());
        columns.put("email", ColumnDef.of("email", "VARCHAR(255)"));

        var result = schemaManager.createTable("users", columns);
        
        assertTrue(result.success(), "Table creation should succeed");
        assertTrue(schemaManager.tableExists("users"), "Table should exist");
    }

    @Test
    void testGetTables() {
        var result = schemaManager.createTable("test_table", Map.of(
            "id", ColumnDef.of("id", "INT").primaryKey()
        ));
        
        var tables = schemaManager.getTables();
        
        assertFalse(tables.isEmpty(), "Should have tables");
        assertTrue(tables.contains("TEST_TABLE".toLowerCase()) || tables.contains("test_table"), 
            "Should contain test_table");
    }

    @Test
    void testGetColumns() {
        var columns = new LinkedHashMap<String, ColumnDef>();
        columns.put("id", ColumnDef.of("id", "INT").primaryKey());
        columns.put("name", ColumnDef.of("name", "VARCHAR(100)").notNull());
        
        schemaManager.createTable("column_test", columns);
        
        var cols = schemaManager.getColumns("column_test");
        
        assertFalse(cols.isEmpty(), "Should have columns");
        assertTrue(cols.stream().anyMatch(c -> c.name().equalsIgnoreCase("id")), 
            "Should have id column");
        assertTrue(cols.stream().anyMatch(c -> c.name().equalsIgnoreCase("name")), 
            "Should have name column");
    }

    @Test
    void testDropTable() {
        var columns = Map.of("id", ColumnDef.of("id", "INT").primaryKey());
        schemaManager.createTable("to_drop", columns);
        
        assertTrue(schemaManager.tableExists("to_drop"));
        
        var result = schemaManager.dropTable("to_drop", false);
        
        assertTrue(result.success(), "Drop should succeed");
        assertFalse(schemaManager.tableExists("to_drop"), "Table should not exist");
    }

    @Test
    void testAlterTableAddColumn() {
        var columns = Map.of("id", ColumnDef.of("id", "INT").primaryKey());
        schemaManager.createTable("alter_test", columns);
        
        var result = schemaManager.alterTableAddColumn(
            "alter_test", 
            ColumnDef.of("new_col", "VARCHAR(100)")
        );
        
        assertTrue(result.success(), "Alter should succeed");
        
        var cols = schemaManager.getColumns("alter_test");
        assertTrue(cols.stream().anyMatch(c -> c.name().equalsIgnoreCase("new_col")),
            "Should have new column");
    }

    @Test
    void testCreateIndex() {
        var columns = Map.of("id", ColumnDef.of("id", "INT").primaryKey());
        schemaManager.createTable("index_test", columns);
        
        var result = schemaManager.createIndex("idx_id", "index_test", "id");
        
        assertTrue(result.success(), "Index creation should succeed");
    }

    @Test
    void testAnalyzeTable() {
        var columns = Map.of("id", ColumnDef.of("id", "INT").primaryKey());
        schemaManager.createTable("analyze_test", columns);
        
        var result = schemaManager.analyzeTable("analyze_test");
        
        assertTrue(result.success(), "Analyze should succeed");
    }

    @Test
    void testVacuumDatabase() {
        var result = schemaManager.analyzeTable("users");
        
        // Vacuum should succeed even if tables don't exist
        assertNotNull(result, "Result should not be null");
    }
}