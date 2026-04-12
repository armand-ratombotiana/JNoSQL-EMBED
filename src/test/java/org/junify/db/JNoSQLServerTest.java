package org.junify.db;

import org.junify.db.nosql.document.Document;
import org.junify.db.console.http.JunifyDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class JunifyDBServerTest {

    private JunifyDB db;
    private JunifyDBServer server;

    @BeforeEach
    void setUp() throws Exception {
        db = JunifyDB.embed().build();
        server = db.startServer(0);
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (db != null && db.isOpen()) db.close();
    }

    private int port() {
        return server.port();
    }

    @Test
    void healthEndpoint() throws Exception {
        var response = get("/api/health");
        assertEquals(200, response.code);
        assertTrue(response.body.contains("ok"));
    }

    @Test
    void insertDocument() throws Exception {
        var response = post("/api/collections/users", "{\"fields\":{\"name\":\"Alice\"},\"id\":null}");
        assertEquals(201, response.code);
        assertTrue(response.body.contains("name"));
    }

    @Test
    void findAllDocuments() throws Exception {
        db.documentCollection("users").insert(Document.of("name", "Alice"));
        db.documentCollection("users").insert(Document.of("name", "Bob"));

        var response = get("/api/collections/users");
        assertEquals(200, response.code);
        assertTrue(response.body.contains("Alice"));
        assertTrue(response.body.contains("Bob"));
    }

    @Test
    void findDocumentById() throws Exception {
        var doc = db.documentCollection("users").insert(Document.of("name", "Alice"));

        var response = get("/api/collections/users/" + doc.id());
        assertEquals(200, response.code);
        assertTrue(response.body.contains("Alice"));
    }

    @Test
    void deleteDocumentById() throws Exception {
        var doc = db.documentCollection("users").insert(Document.of("name", "Alice"));

        var response = delete("/api/collections/users/" + doc.id());
        assertEquals(204, response.code);

        assertNull(db.documentCollection("users").findById(doc.id()));
    }

    @Test
    void kvPutAndGet() throws Exception {
        var put = put("/api/kv/cache/session1", "{\"value\":\"user-data\"}");
        assertEquals(201, put.code);

        var get = get("/api/kv/cache/session1");
        assertEquals(200, get.code);
        assertTrue(get.body.contains("user-data"));
    }

    @Test
    void kvDelete() throws Exception {
        db.keyValueBucket("cache").put("key1", "value1");

        var response = delete("/api/kv/cache/key1");
        assertEquals(204, response.code);

        assertNull(db.keyValueBucket("cache").get("key1"));
    }

    private record Response(int code, String body) {}

    private Response get(String path) throws Exception {
        var conn = (HttpURLConnection) new URL("http://localhost:" + port() + path).openConnection();
        conn.setRequestMethod("GET");
        return readResponse(conn);
    }

    private Response post(String path, String body) throws Exception {
        var conn = (HttpURLConnection) new URL("http://localhost:" + port() + path).openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        try (var os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return readResponse(conn);
    }

    private Response put(String path, String body) throws Exception {
        var conn = (HttpURLConnection) new URL("http://localhost:" + port() + path).openConnection();
        conn.setRequestMethod("PUT");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        try (var os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return readResponse(conn);
    }

    private Response delete(String path) throws Exception {
        var conn = (HttpURLConnection) new URL("http://localhost:" + port() + path).openConnection();
        conn.setRequestMethod("DELETE");
        return readResponse(conn);
    }

    private Response readResponse(HttpURLConnection conn) throws Exception {
        int code = conn.getResponseCode();
        var stream = code >= 400 ? conn.getErrorStream() : conn.getInputStream();
        if (stream == null) return new Response(code, "");
        try (var reader = new BufferedReader(new InputStreamReader(stream))) {
            var body = reader.lines().reduce("", (a, b) -> a + b);
            return new Response(code, body);
        }
    }
}
