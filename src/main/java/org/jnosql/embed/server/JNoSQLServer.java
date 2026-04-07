package org.jnosql.embed.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.jnosql.embed.JNoSQL;
import org.jnosql.embed.document.Document;
import org.jnosql.embed.document.DocumentCollection;
import org.jnosql.embed.document.Query;
import org.jnosql.embed.util.JsonSerde;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JNoSQLServer {

    private final JNoSQL db;
    private HttpServer server;

    public JNoSQLServer(JNoSQL db) {
        this.db = db;
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/api/collections", new CollectionsHandler());
        server.createContext("/api/collections/", new CollectionHandler());
        server.createContext("/api/kv/", new KeyValueHandler());
        server.createContext("/api/health", new HealthHandler());
        server.setExecutor(null);
        server.start();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            sendJson(exchange, 200, Map.of("status", "ok", "open", db.isOpen()));
        }
    }

    private class CollectionsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                var collections = db.config().storageEngine().create(db.config().dataDir());
                sendJson(exchange, 200, Map.of("collections", "use /api/collections/{name}"));
            }
        }
    }

    private class CollectionHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            if (parts.length < 5) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/collections/{name}[/id]"));
                return;
            }
            var name = parts[4];
            var collection = db.documentCollection(name);

            if (parts.length == 5) {
                if ("GET".equals(exchange.getRequestMethod())) {
                    sendJson(exchange, 200, collection.findAll());
                } else if ("POST".equals(exchange.getRequestMethod())) {
                    var body = readBody(exchange);
                    var doc = Document.fromJson(body);
                    var saved = collection.insert(doc);
                    sendJson(exchange, 201, saved);
                }
            } else {
                var id = parts[5];
                if ("GET".equals(exchange.getRequestMethod())) {
                    var doc = collection.findById(id);
                    if (doc != null) sendJson(exchange, 200, doc);
                    else sendJson(exchange, 404, Map.of("error", "Not found"));
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    collection.deleteById(id);
                    sendJson(exchange, 204, null);
                }
            }
        }
    }

    private class KeyValueHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            if (parts.length < 5) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/kv/{bucket}/{key}"));
                return;
            }
            var bucketName = parts[4];
            var bucket = db.keyValueBucket(bucketName);

            if (parts.length >= 6) {
                var key = parts[5];
                if ("GET".equals(exchange.getRequestMethod())) {
                    var value = bucket.get(key);
                    if (value != null) sendJson(exchange, 200, Map.of("key", key, "value", value));
                    else sendJson(exchange, 404, Map.of("error", "Not found"));
                } else if ("PUT".equals(exchange.getRequestMethod()) || "POST".equals(exchange.getRequestMethod())) {
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    bucket.put(key, data.getOrDefault("value", "").toString());
                    sendJson(exchange, 201, Map.of("key", key, "status", "created"));
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    bucket.delete(key);
                    sendJson(exchange, 204, null);
                }
            }
        }
    }

    private void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        var json = body != null ? JsonSerde.toJson(body) : "";
        var bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (var os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private String readBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
