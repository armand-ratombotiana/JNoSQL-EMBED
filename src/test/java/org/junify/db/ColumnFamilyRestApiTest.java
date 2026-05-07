package org.junify.db;

import org.junify.db.console.http.JunifyDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ColumnFamily REST API endpoints
 */
class ColumnFamilyRestApiTest {

    private JunifyDB db;
    private JunifyDBServer server;
    private HttpClient httpClient;
    private String baseUrl;

    @BeforeEach
    void setUp() throws Exception {
        db = JunifyDB.embed().build();
        server = db.startServer(0); // Use random available port
        baseUrl = "http://localhost:" + server.port();
        httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

        // Initialize test data
        setupTestData();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
        if (db != null) {
            db.close();
        }
    }

    private void setupTestData() throws Exception {
        // Create test column family with sample data
        var cf = db.columnFamily("test_api");

        // Row with permanent columns
        cf.put("user:1", "name", "Alice");
        cf.put("user:1", "email", "alice@example.com");
        cf.put("user:1", "age", 30);

        // Row with TTL columns
        cf.put("user:2", "name", "Bob");
        cf.put("user:2", "session_token", "abc123", 3600);

        // Time-series data
        for (int i = 0; i < 20; i++) {
            cf.put("metrics:cpu", "ts:" + String.format("%04d", i), Math.random() * 100);
        }
    }

    // ==================== BASIC ENDPOINT TESTS ====================

    @Test
    @DisplayName("REST-01: GET /api/columns/{family} - List row keys")
    void testListRowKeys() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        var body = response.body();
        assertTrue(body.contains("test_api"));
        assertTrue(body.contains("user:1"));
        assertTrue(body.contains("user:2"));
    }

    @Test
    @DisplayName("REST-02: GET /api/columns/{family}/{row} - Get full row")
    void testGetFullRow() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:1"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        var body = response.body();
        assertTrue(body.contains("Alice"));
        assertTrue(body.contains("alice@example.com"));
    }

    @Test
    @DisplayName("REST-03: POST /api/columns/{family}/{row} - Create row")
    void testCreateRow() throws Exception {
        var request = HttpRequest.newBuilder()
            .POST(HttpRequest.BodyPublishers.ofString(
                "{\"name\":\"Charlie\",\"email\":\"charlie@example.com\"}"))
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:3"))
            .header("Content-Type", "application/json")
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());

        // Verify the row was created
        var getRequest = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:3"))
            .build();
        var getResponse = httpClient.send(getRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, getResponse.statusCode());
        assertTrue(getResponse.body().contains("Charlie"));
    }

    @Test
    @DisplayName("REST-04: DELETE /api/columns/{family}/{row} - Delete row")
    void testDeleteRow() throws Exception {
        var request = HttpRequest.newBuilder()
            .DELETE()
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:1"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(204, response.statusCode());

        // Verify the row was deleted
        var getRequest = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:1"))
            .build();
        var getResponse = httpClient.send(getRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, getResponse.statusCode());
    }

    // ==================== STATISTICS ENDPOINT TESTS ====================

    @Test
    @DisplayName("REST-05: GET /api/columns/{family}/stats - Family stats")
    void testFamilyStats() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/stats"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        var body = response.body();
        assertTrue(body.contains("familyName"));
        assertTrue(body.contains("rowCount"));
        assertTrue(body.contains("totalColumns"));
    }

    @Test
    @DisplayName("REST-06: GET /api/columns/{family}/{row}/stats - Row stats")
    void testRowStats() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:1/stats"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        var body = response.body();
        assertTrue(body.contains("user:1"));
        assertTrue(body.contains("columnCount"));
    }

    // ==================== PAGINATION ENDPOINT TESTS ====================

    @Test
    @DisplayName("REST-07: GET /api/columns/{family}/{row}/get-range - Paginated retrieval")
    void testPaginatedRetrieval() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/metrics:cpu/get-range?limit=5&offset=0"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        var body = response.body();
        assertTrue(body.contains("limit"));
        assertTrue(body.contains("offset"));
        assertTrue(body.contains("columnsReturned"));

        // Verify only 5 columns returned
        var dataStart = body.indexOf("\"data\"");
        var dataEnd = body.indexOf("}", dataStart);
        var dataSection = body.substring(dataStart, Math.min(dataEnd + 100, body.length()));
    }

    @Test
    @DisplayName("REST-08: GET /api/columns/{family}/{row}/get-range - With offset")
    void testPaginatedRetrievalWithOffset() throws Exception {
        var request1 = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/metrics:cpu/get-range?limit=5&offset=0"))
            .build();
        var response1 = httpClient.send(request1, HttpResponse.BodyHandlers.ofString());

        var request2 = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/metrics:cpu/get-range?limit=5&offset=5"))
            .build();
        var response2 = httpClient.send(request2, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response1.statusCode());
        assertEquals(200, response2.statusCode());

        // Pages should be different
        assertNotEquals(response1.body(), response2.body());
    }

    // ==================== FILTERING ENDPOINT TESTS ====================

    @Test
    @DisplayName("REST-09: GET /api/columns/{family}/{row}/filter - Filter by columns")
    void testFilterByColumns() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:1/filter?columns=name,email"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        var body = response.body();
        assertTrue(body.contains("Alice"));
        assertTrue(body.contains("alice@example.com"));
        assertFalse(body.contains("age"));
    }

    @Test
    @DisplayName("REST-10: GET /api/columns/{family}/{row}/filter-prefix - Filter by prefix")
    void testFilterByPrefix() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/metrics:cpu/filter-prefix?prefix=ts:00"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        var body = response.body();
        assertTrue(body.contains("ts:00"));
    }

    @Test
    @DisplayName("REST-11: GET /api/columns/{family}/{row}/filter-pattern - Filter by regex")
    void testFilterByPattern() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/metrics:cpu/filter-pattern?pattern=ts:.*0[0-5]$"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
    }

    // ==================== TTL ENDPOINT TESTS ====================

    @Test
    @DisplayName("REST-12: GET /api/columns/{family}/{row}/ttl/{column} - Get TTL")
    void testGetTtl() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:2/ttl/session_token"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        var body = response.body();
        assertTrue(body.contains("remainingTtlSeconds"));
        assertTrue(body.contains("hasTtl"));
        assertTrue(body.contains("true")); // session_token has TTL
    }

    @Test
    @DisplayName("REST-13: PUT /api/columns/{family}/{row}/column/{column} - Put with TTL")
    void testPutWithTtl() throws Exception {
        var request = HttpRequest.newBuilder()
            .PUT(HttpRequest.BodyPublishers.ofString(
                "{\"value\":\"temp_value\",\"ttlSeconds\":60}"))
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:1/column/temp_col"))
            .header("Content-Type", "application/json")
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());

        // Verify TTL was set
        var ttlRequest = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:1/ttl/temp_col"))
            .build();
        var ttlResponse = httpClient.send(ttlRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, ttlResponse.statusCode());
        assertTrue(ttlResponse.body().contains("hasTtl"));
        assertTrue(ttlResponse.body().contains("true"));
    }

    @Test
    @DisplayName("REST-14: POST /api/columns/{family}/{row} - Nested TTL format")
    void testNestedTtlFormat() throws Exception {
        var request = HttpRequest.newBuilder()
            .POST(HttpRequest.BodyPublishers.ofString(
                "{\"session\":{\"value\":\"xyz789\",\"ttlSeconds\":120}}"))
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:1"))
            .header("Content-Type", "application/json")
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());

        // Verify TTL was set
        var ttlRequest = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:1/ttl/session"))
            .build();
        var ttlResponse = httpClient.send(ttlRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, ttlResponse.statusCode());
        assertTrue(ttlResponse.body().contains("hasTtl"));
        assertTrue(ttlResponse.body().contains("true"));
    }

    // ==================== CLEANUP ENDPOINT TESTS ====================

    @Test
    @DisplayName("REST-15: POST /api/columns/{family}/{row}/cleanup - Cleanup expired")
    void testCleanupExpired() throws Exception {
        // First create an expired column
        var cf = db.columnFamily("test_api");
        cf.put("cleanup_test", "temp", "value", 1);

        Thread.sleep(1100);

        var request = HttpRequest.newBuilder()
            .POST(HttpRequest.BodyPublishers.ofString("{}"))
            .uri(URI.create(baseUrl + "/api/columns/test_api/cleanup_test/cleanup"))
            .header("Content-Type", "application/json")
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        var body = response.body();
        assertTrue(body.contains("deleted"));
        assertTrue(body.contains("1"));
    }

    // ==================== ERROR HANDLING TESTS ====================

    @Test
    @DisplayName("REST-16: GET non-existent row returns 404")
    void testNonExistentRow() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/nonexistent"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
    }

    @Test
    @DisplayName("REST-17: GET non-existent column TTL returns 404")
    void testNonExistentColumnTtl() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:1/ttl/nonexistent"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
    }

    @Test
    @DisplayName("REST-18: Missing columns parameter returns 400")
    void testMissingColumnsParam() throws Exception {
        var request = HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(baseUrl + "/api/columns/test_api/user:1/filter"))
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, response.statusCode());
    }

    @Test
    @DisplayName("REST-19: Invalid method returns 405")
    void testInvalidMethod() throws Exception {
        var request = HttpRequest.newBuilder()
            .PUT(HttpRequest.BodyPublishers.ofString("{}"))
            .uri(URI.create(baseUrl + "/api/columns/test_api/stats"))
            .header("Content-Type", "application/json")
            .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(405, response.statusCode());
    }
}
