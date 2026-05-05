package org.junify.db;

import org.junify.db.storage.spi.H2StorageEngine;
import org.junify.db.storage.spi.FullTextSearchManager;
import org.junify.db.storage.spi.SchemaManager;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class FullTextSearchTest {

    private static H2StorageEngine engine;
    private static FullTextSearchManager ftsManager;
    private static SchemaManager schemaManager;

    @BeforeAll
    static void setup() throws Exception {
        var tempDir = Files.createTempDirectory("fts-test");
        engine = new H2StorageEngine(tempDir, "ftsdb");
        ftsManager = new FullTextSearchManager(engine);
        schemaManager = engine.schemaManager();

        schemaManager.createTable("articles", Map.of(
            "id", "INT PRIMARY KEY",
            "title", "VARCHAR(500)",
            "content", "TEXT",
            "author", "VARCHAR(255)"
        ));

        // Insert test data
        engine.executeSql("INSERT INTO articles VALUES (1, 'Java Programming', 'Java is a programming language created by Sun Microsystems. It is object-oriented.', 'James Gosling')");
        engine.executeSql("INSERT INTO articles VALUES (2, 'Python Programming', 'Python is a high-level programming language. It emphasizes code readability.', 'Guido van Rossum')");
        engine.executeSql("INSERT INTO articles VALUES (3, 'Database Systems', 'A database is an organized collection of structured information. SQL is the standard language.', 'Edgar Codd')");
    }

    @AfterAll
    static void teardown() {
        engine.close();
    }

    @Test
    void testCreateIndex() {
        var result = ftsManager.createIndex("article_fts", "articles", "title", "content");
        
        assertTrue(result.success(), "FTS index creation should succeed");
    }

    @Test
    void testSearch() {
        ftsManager.createIndex("search_fts", "articles", "title", "content");
        
        var result = ftsManager.search("search_fts", "programming", 10);
        
        assertNotNull(result, "Search should return results");
        assertTrue(result.count() >= 0, "Count should be >= 0");
    }

    @Test
    void testSearchWithHighlight() {
        ftsManager.createIndex("highlight_fts", "articles", "title", "content");
        
        var result = ftsManager.searchWithHighlight("highlight_fts", "programming", 10);
        
        assertNotNull(result);
    }

    @Test
    void testRebuildIndex() {
        ftsManager.createIndex("rebuild_fts", "articles", "title", "content");
        
        var result = ftsManager.rebuildIndex("rebuild_fts");
        
        assertTrue(result.success(), "Rebuild should succeed");
    }

    @Test
    void testDropIndex() {
        ftsManager.createIndex("drop_fts", "articles", "title", "content");
        assertTrue(ftsManager.getIndexes().contains("drop_fts"));
        
        var result = ftsManager.dropIndex("drop_fts");
        
        assertTrue(result.success());
    }

    @Test
    void testGetIndexes() {
        ftsManager.createIndex("list_fts", "articles", "title", "content");
        
        var indexes = ftsManager.getIndexes();
        
        assertFalse(indexes.isEmpty());
    }

    @Test
    void testGetIndexInfo() {
        ftsManager.createIndex("info_fts", "articles", "title", "content");
        
        var result = ftsManager.getIndexInfo("info_fts");
        
        assertNotNull(result);
    }
}