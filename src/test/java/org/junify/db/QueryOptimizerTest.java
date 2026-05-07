package org.junify.db;

import org.junify.db.storage.spi.H2StorageEngine;
import org.junify.db.storage.spi.QueryOptimizer;
import org.junify.db.storage.spi.SchemaManager;
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
            "id", "INT PRIMARY KEY",
            "customer_id", "INT",
            "total", "DECIMAL(10,2)"
        ));
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
    void testExplain() {
        var plan = optimizer.explain("SELECT * FROM orders WHERE id = 1");

        assertNotNull(plan, "Query plan should not be null");
        assertTrue(plan.success(), "Explain should succeed");
    }

    @Test
    void testIsOptimized() {
        // Query with WHERE clause but no explicit index hint - not optimized per current logic
        assertFalse(optimizer.isOptimized("SELECT * FROM orders WHERE id = 1"),
            "Query without index hint should not be considered optimized");

        // Simple SELECT without WHERE - should be optimized (no filtering needed)
        assertTrue(optimizer.isOptimized("SELECT * FROM orders"),
            "Simple SELECT without WHERE should be optimized");
    }

    @Test
    void testAnalyzeQuery() {
        var result = optimizer.analyzeQuery("SELECT COUNT(*) FROM orders");

        assertNotNull(result, "Query analysis result should not be null");
        assertTrue(result.success(), "Query analysis should succeed");
    }

    @Test
    void testSuggestIndexes() {
        var result = optimizer.suggestIndexes("orders", 100);

        assertNotNull(result, "Index suggestion result should not be null");
        assertTrue(result.success(), "Index suggestion should succeed");
    }

    @Test
    void testTableStats() {
        var result = optimizer.tableStats("orders");

        assertNotNull(result, "Table stats result should not be null");
        assertTrue(result.success(), "Table stats should succeed");
    }
}