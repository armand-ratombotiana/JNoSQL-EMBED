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
import java.io.OutputStream;
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
        server.createContext("/", new StaticHandler());
        server.createContext("/api/collections", new CollectionsHandler());
        server.createContext("/api/collections/", new CollectionHandler());
        server.createContext("/api/kv/", new KeyValueHandler());
        server.createContext("/api/columns/", new ColumnHandler());
        server.createContext("/api/health", new HealthHandler());
        server.createContext("/api/metrics", new MetricsHandler());
        server.createContext("/api/stats", new StatsHandler());
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

    private class StaticHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            var path = exchange.getRequestURI().getPath();
            if (path.equals("/")) {
                path = "/index.html";
            }
            var resourcePath = "/static" + path;
            try (var is = JNoSQLServer.class.getResourceAsStream(resourcePath)) {
                if (is == null) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                var bytes = is.readAllBytes();
                exchange.getResponseHeaders().set("Content-Type", getContentType(path));
                exchange.sendResponseHeaders(200, bytes.length);
                try (var os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
        }

        private String getContentType(String path) {
            if (path.endsWith(".html")) return "text/html";
            if (path.endsWith(".css")) return "text/css";
            if (path.endsWith(".js")) return "application/javascript";
            if (path.endsWith(".json")) return "application/json";
            if (path.endsWith(".png")) return "image/png";
            if (path.endsWith(".ico")) return "image/x-icon";
            return "text/plain";
        }
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
                sendJson(exchange, 200, Map.of("collections", "use /api/collections/{name}"));
            }
        }
    }

    private class CollectionHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            if (parts.length < 4) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/collections/{name}[/id]"));
                return;
            }
            var name = parts[3];
            var collection = db.documentCollection(name);

            if (parts.length == 4) {
                if ("GET".equals(exchange.getRequestMethod())) {
                    sendJson(exchange, 200, collection.findAll());
                } else if ("POST".equals(exchange.getRequestMethod())) {
                    var body = readBody(exchange);
                    var doc = Document.fromJson(body);
                    var saved = collection.insert(doc);
                    sendJson(exchange, 201, saved);
                }
            } else {
                var id = parts[4];
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
            if (parts.length < 4) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/kv/{bucket}[/{key}]"));
                return;
            }
            var bucketName = parts[3];
            var bucket = db.keyValueBucket(bucketName);

            if (parts.length >= 5) {
                var key = parts[4];
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
            } else {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/kv/{bucket}/{key}"));
            }
        }
    }

    private class ColumnHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            if (parts.length < 4) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/columns/{name}/{key}"));
                return;
            }
            var name = parts[3];
            var cf = db.columnFamily(name);

            if (parts.length >= 5) {
                var key = parts[4];
                if ("GET".equals(exchange.getRequestMethod())) {
                    var row = cf.getRow(key);
                    if (row != null && !row.isEmpty()) sendJson(exchange, 200, Map.of("key", key, "columns", row));
                    else sendJson(exchange, 404, Map.of("error", "Not found"));
                } else if ("PUT".equals(exchange.getRequestMethod()) || "POST".equals(exchange.getRequestMethod())) {
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    for (Object o : data.entrySet()) {
                        var entry = (java.util.Map.Entry<?, ?>) o;
                        cf.put(key, entry.getKey().toString(), entry.getValue());
                    }
                    sendJson(exchange, 201, Map.of("key", key, "status", "created"));
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    cf.deleteRow(key);
                    sendJson(exchange, 204, null);
                }
            } else {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/columns/{name}/{key}"));
            }
        }
    }

    private void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        var json = body != null ? JsonSerde.toJson(body) : "";
        var bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (var os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                sendJson(exchange, 200, db.metrics().snapshot());
            }
        }
    }

    private class StatsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                var runtime = Runtime.getRuntime();
                var memory = Map.of(
                        "totalMemory", runtime.totalMemory(),
                        "freeMemory", runtime.freeMemory(),
                        "usedMemory", runtime.totalMemory() - runtime.freeMemory(),
                        "maxMemory", runtime.maxMemory(),
                        "availableProcessors", runtime.availableProcessors()
                );
                sendJson(exchange, 200, Map.of(
                        "database", Map.of("open", db.isOpen(), "engine", db.config().storageEngine().name()),
                        "memory", memory,
                        "threads", Map.of("activeCount", Thread.activeCount())
                ));
            }
        }
    }

    private String readBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
