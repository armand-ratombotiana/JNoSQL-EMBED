package org.junify.db;

import org.junify.db.storage.spi.H2StorageEngine;
import org.junify.db.storage.spi.QueryOptimizer;
import org.junify.db.storage.spi.SchemaManager;
import org.junify.db.storage.spi.SchemaManager.ColumnDef;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class QueryOptimizerTest {

    private static H2StorageEngine engine;
    private static QueryOptimizer optimizer;
    private static SchemaManager schemaManager;

    @BeforeAll
    static void setup() throws Exception {
        var tempDir = Files.createTempDirectory("optimizer-test");
        engine = new H2StorageEngine(tempDir, "optimizerdb");
        optimizer = engine.queryOptimizer();
        schemaManager = engine.schemaManager();
        
        schemaManager.createTable("orders", Map.of(
            "id", ColumnDef.of("id", "INT").primaryKey(),
            "customer_id", ColumnDef.of("customer_id", "INT"),
            "total", ColumnDef.of("total", "DECIMAL(10,2)")
        ));
    }

    @AfterAll
    static void teardown() {
        engine.close();
    }

    @Test
    void testExplain() {
        var plan = optimizer.explain("SELECT * FROM orders WHERE id = 1");
        
        assertNotNull(plan);
    }

    @Test
    void testIsOptimized() {
        // Simple SELECT without WHERE should not be optimized
        assertFalse(optimizer.isOptimized("SELECT * FROM orders"));
        
        // SELECT with WHERE should be optimized
        assertTrue(optimizer.isOptimized("SELECT * FROM orders WHERE id = 1"));
    }

    @Test
    void testAnalyzeQuery() {
        var result = optimizer.analyzeQuery("SELECT COUNT(*) FROM orders");
        
        assertNotNull(result);
    }

    @Test
    void testSuggestIndexes() {
        var result = optimizer.suggestIndexes("orders", 100);
        
        assertNotNull(result);
    }

    @Test
    void testTableStats() {
        var result = optimizer.tableStats("orders");
        
        assertNotNull(result);
    }
}