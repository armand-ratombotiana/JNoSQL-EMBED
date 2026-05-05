package org.junify.db.console.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.document.Query;
import org.junify.db.core.util.JsonSerde;
import org.junify.db.storage.spi.SchemaManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

public class JunifyDBServer {

    private final JunifyDB db;
    private HttpServer server;
    private long startTime;
    private String apiKey;
    private boolean authEnabled = false;
    private boolean corsEnabled = true;
    private boolean compressionEnabled = true;
    private int rateLimit = 1000;
    private Map<String, RateLimitEntry> rateLimitMap = new ConcurrentHashMap<>();
    
    private static class RateLimitEntry {
        AtomicInteger count = new AtomicInteger(0);
        long windowStart = System.currentTimeMillis();
    }

    public JunifyDBServer(JunifyDB db) {
        this.db = db;
    }

    public void setApiKey(String apiKey) {
        if (apiKey != null && !apiKey.isEmpty()) {
            this.apiKey = apiKey;
            this.authEnabled = true;
        }
    }

    private boolean isAuthValid(HttpExchange exchange) {
        if (!authEnabled) return true;
        var authHeader = exchange.getRequestHeaders().getFirst("X-API-Key");
        return apiKey != null && apiKey.equals(authHeader);
    }

    private void sendAuthError(HttpExchange exchange) throws IOException {
        sendJson(exchange, 401, Map.of("error", "Unauthorized", "message", "Invalid or missing API key"));
    }

    private boolean isRateLimited(HttpExchange exchange) {
        var clientIp = getClientIp(exchange);
        var now = System.currentTimeMillis();
        var entry = rateLimitMap.computeIfAbsent(clientIp, k -> new RateLimitEntry());
        
        if (now - entry.windowStart > 60000) {
            entry.windowStart = now;
            entry.count.set(0);
        }
        
        return entry.count.incrementAndGet() > rateLimit;
    }

    private void sendRateLimitError(HttpExchange exchange) throws IOException {
        sendJson(exchange, 429, Map.of("error", "Too Many Requests", "message", "Rate limit exceeded. Try again later."));
    }

    private String getClientIp(HttpExchange exchange) {
        var forwarded = exchange.getRequestHeaders().getFirst("X-Forwarded-For");
        if (forwarded != null) return forwarded.split(",")[0].trim();
        return exchange.getRemoteAddress().getAddress().getHostAddress();
    }

    private void addCorsHeaders(HttpExchange exchange) {
        if (corsEnabled) {
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
            exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, Authorization");
        }
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        startTime = System.currentTimeMillis();

        server.createContext("/", new StaticHandler());
        server.createContext("/api/collections/", new CollectionsHandler());
        server.createContext("/api/kv/", new KeyValueHandler());
        server.createContext("/api/columns/", new ColumnHandler());
        server.createContext("/api/health", new HealthHandler());
        server.createContext("/api/metrics", new MetricsHandler());
        server.createContext("/api/metrics/stream", new MetricsStreamHandler());
        server.createContext("/api/stats", new StatsHandler());
        server.createContext("/api/backup", new BackupHandler());
        server.createContext("/api/indexes/", new IndexHandler());
        server.createContext("/api/transactions", new TransactionHandler());
        server.createContext("/api/schema/", new SchemaHandler());
        server.createContext("/api/vectors/", new VectorHandler());
        server.createContext("/api/sql", new SqlHandler());
        server.createContext("/api/bulk", new BulkHandler());
        server.createContext("/api/cdc", new CDCHandler());
        server.createContext("/api/tables/", new TablesHandler());
        server.createContext("/api/constraints/", new ConstraintsHandler());

        if (corsEnabled) {
            server.createContext("/api/cors", new CorsPreflightHandler());
        }

        server.setExecutor(null);
        server.start();
    }
    
    private class CorsPreflightHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            exchange.getResponseHeaders().set("Access-Control-Max-Age", "3600");
            exchange.sendResponseHeaders(204, -1);
        }
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
            try (var is = JunifyDBServer.class.getResourceAsStream(resourcePath)) {
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
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var runtime = Runtime.getRuntime();
            var totalMem = runtime.totalMemory();
            var freeMem = runtime.freeMemory();
            
            var health = Map.of(
                "status", "ok",
                "open", db.isOpen(),
                "version", "1.0.0",
                "engine", db.config().storageEngine().name(),
                "uptime", System.currentTimeMillis() - startTime,
                "timestamp", System.currentTimeMillis(),
                "memory", Map.of(
                    "used", totalMem - freeMem,
                    "total", totalMem,
                    "max", runtime.maxMemory(),
                    "free", freeMem
                ),
                "threads", Map.of(
                    "active", Thread.activeCount(),
                    "daemon", Thread.activeCount()
                )
            );
            sendJson(exchange, 200, health);
        }
    }

    private class CollectionsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            if ("GET".equals(exchange.getRequestMethod())) {
                sendJson(exchange, 200, Map.of("collections", "use /api/collections/{name}"));
            }
        }
    }

    private class CollectionHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
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
                    try {
                        var body = readBody(exchange);
                        var doc = Document.fromJson(body);
                        var saved = collection.insert(doc);
                        sendJson(exchange, 201, saved);
                    } catch (Exception e) {
                        sendJson(exchange, 500, Map.of("error", "Internal server error", "message", e.getMessage()));
                    }
                }
            } else {
                var id = parts[4];
                if ("GET".equals(exchange.getRequestMethod())) {
                    var doc = collection.findById(id);
                    if (doc != null) sendJson(exchange, 200, doc);
                    else sendJson(exchange, 404, Map.of("error", "Not found"));
                } else if ("PUT".equals(exchange.getRequestMethod()) || "POST".equals(exchange.getRequestMethod())) {
                    try {
                        var body = readBody(exchange);
                        var data = JsonSerde.fromJson(body, Map.class);
                        var doc = Document.of("name", "temp");
                        doc.id(id);
                        for (var entry : data.entrySet()) {
                            var e = (java.util.Map.Entry<?, ?>) entry;
                            doc.add(e.getKey().toString(), e.getValue());
                        }
                        var saved = collection.insert(doc);
                        sendJson(exchange, 201, saved);
                    } catch (Exception e) {
                        sendJson(exchange, 500, Map.of("error", "Internal server error", "message", e.getMessage()));
                    }
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
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
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
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
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

    private class BackupHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            
            if ("GET".equals(exchange.getRequestMethod()) && parts.length == 3) {
                var metrics = db.metrics().snapshot();
                var collections = metrics.containsKey("collections") ? metrics.get("collections") : Map.of();
                sendJson(exchange, 200, Map.of(
                    "backup", Map.of(
                        "description", "Use POST /api/backup to create backup",
                        "restore", "Use POST /api/backup/restore with JSON body containing 'backupFile' path"
                    ),
                    "diskUsage", getDiskUsage(),
                    "collections", collections
                ));
                return;
            }
            
            if ("POST".equals(exchange.getRequestMethod()) && parts.length == 3) {
                var body = readBody(exchange);
                var data = JsonSerde.fromJson(body, Map.class);
                
                if (data.containsKey("backupFile")) {
                    var backupManager = new org.junify.db.core.backup.BackupManager(db.config().storageEngine().create(
                        db.config().dataDir(), true, 1000));
                    var backupFile = java.nio.file.Paths.get(data.get("backupFile").toString());
                    backupManager.restore(backupFile);
                    sendJson(exchange, 200, Map.of("status", "restored", "file", backupFile.toString()));
                } else {
                    var backupDir = java.nio.file.Files.createTempDirectory("junify-backup");
                    var backupManager = new org.junify.db.core.backup.BackupManager(
                        new org.junify.db.storage.spi.FileEngine(backupDir, 1000, false));
                    var backupFile = backupManager.backup(backupDir);
                    sendJson(exchange, 200, Map.of(
                        "status", "backup created",
                        "file", backupFile.toString(),
                        "size", java.nio.file.Files.size(backupFile)
                    ));
                }
                return;
            }
            
            sendJson(exchange, 400, Map.of("error", "Usage: GET /api/backup or POST /api/backup"));
        }
        
        private Map<String, Object> getDiskUsage() {
            try {
                var dataDir = db.config().dataDir();
                long totalSize = 0;
                int fileCount = 0;
                
                if (java.nio.file.Files.exists(dataDir)) {
                    try (var stream = java.nio.file.Files.list(dataDir)) {
                        var files = stream.filter(p -> p.toString().endsWith(".json")).toList();
                        for (var file : files) {
                            totalSize += java.nio.file.Files.size(file);
                            fileCount++;
                        }
                    }
                }
                
                return Map.of(
                    "dataDir", dataDir.toString(),
                    "totalBytes", totalSize,
                    "fileCount", fileCount,
                    "totalMB", String.format("%.2f MB", totalSize / 1024.0 / 1024.0)
                );
            } catch (IOException e) {
                return Map.of("error", e.getMessage());
            }
        }
    }

    private class IndexHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            if (parts.length < 4) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/indexes/{collection}"));
                return;
            }
            var collectionName = parts[3];
            var collection = db.documentCollection(collectionName);
            
            if ("GET".equals(exchange.getRequestMethod())) {
                var indexes = collection.getIndexes();
                var result = new java.util.HashMap<String, Object>();
                result.put("collection", collectionName);
                result.put("indexes", indexes);
                sendJson(exchange, 200, result);
            } else if ("POST".equals(exchange.getRequestMethod())) {
                var body = readBody(exchange);
                var data = JsonSerde.fromJson(body, Map.class);
                var field = data.get("field").toString();
                var index = collection.createIndex(field);
                sendJson(exchange, 201, Map.of(
                    "status", "created",
                    "collection", collectionName,
                    "field", field
                ));
            } else if ("DELETE".equals(exchange.getRequestMethod())) {
                collection.clear();
                sendJson(exchange, 200, Map.of("status", "indexes cleared"));
            }
        }
    }

    private class TransactionHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            if ("POST".equals(exchange.getRequestMethod())) {
                var tx = db.beginTransaction();
                var txId = tx.hashCode();
                activeTransactions.put(txId, tx);
                sendJson(exchange, 200, Map.of(
                    "transactionId", txId,
                    "status", "started"
                ));
            } else {
                sendJson(exchange, 400, Map.of("error", "POST /api/transactions to begin"));
            }
        }
    }

    private class SchemaHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            if (parts.length < 4) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/schema/{collection}"));
                return;
            }
            var collectionName = parts[3];
            
            if ("GET".equals(exchange.getRequestMethod())) {
                var schema = schemaValidator.getSchema(collectionName);
                sendJson(exchange, 200, Map.of(
                    "collection", collectionName,
                    "hasSchema", schema != null,
                    "schema", schema != null ? schema.getFields() : java.util.List.of()
                ));
            } else if ("POST".equals(exchange.getRequestMethod())) {
                var body = readBody(exchange);
                var data = JsonSerde.fromJson(body, Map.class);
                var schema = org.junify.db.core.schema.SchemaValidator.builder(collectionName);
                
                if (data.containsKey("strict")) {
                    var constructor = schema.getClass().getDeclaredConstructors()[0];
                    constructor.setAccessible(true);
                }
                
                schemaValidator.registerSchema(collectionName, schema);
                sendJson(exchange, 201, Map.of(
                    "status", "schema registered",
                    "collection", collectionName
                ));
            }
        }
    }

    private java.util.Map<Integer, org.junify.db.transaction.mvcc.Transaction> activeTransactions = new java.util.concurrent.ConcurrentHashMap<>();
    private org.junify.db.core.schema.SchemaValidator schemaValidator = new org.junify.db.core.schema.SchemaValidator();
    private java.util.Map<String, org.junify.db.index.hnsw.HNSWIndex> vectorIndexes = new java.util.concurrent.ConcurrentHashMap<>();

    private class VectorHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            if (parts.length < 5) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/vectors/{index}[/id]"));
                return;
            }
            var indexName = parts[3];
            var hnsw = vectorIndexes.computeIfAbsent(indexName, k -> new org.junify.db.index.hnsw.HNSWIndex(128));
            
            if (parts.length == 5) {
                if ("GET".equals(exchange.getRequestMethod())) {
                    sendJson(exchange, 200, Map.of(
                        "index", indexName,
                        "dimensions", hnsw.dimensions(),
                        "size", hnsw.size()
                    ));
                    return;
                }
            }
            
            var id = parts[4];
            
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var vector = parseVector((java.util.List<?>) data.get("vector"));
                    var k = data.containsKey("k") ? ((Number) data.get("k")).intValue() : 5;
                    var results = hnsw.search(vector, k);
                    sendJson(exchange, 200, Map.of(
                        "query", id,
                        "results", results
                    ));
                } catch (Exception e) {
                    sendJson(exchange, 500, Map.of("error", "Search failed", "message", e.getMessage()));
                }
            } else if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var vector = parseVector((java.util.List<?>) data.get("vector"));
                    hnsw.add(id, vector);
                    sendJson(exchange, 201, Map.of("id", id, "status", "added"));
                } catch (Exception e) {
                    sendJson(exchange, 500, Map.of("error", "Insert failed", "message", e.getMessage()));
                }
            } else if ("DELETE".equals(exchange.getRequestMethod())) {
                try {
                    hnsw.remove(id);
                    sendJson(exchange, 204, null);
                } catch (Exception e) {
                    sendJson(exchange, 500, Map.of("error", "Delete failed", "message", e.getMessage()));
                }
            }
        }
        
        private float[] parseVector(java.util.List<?> list) {
            float[] vector = new float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                vector[i] = ((Number) list.get(i)).floatValue();
            }
            return vector;
        }
    }

    private void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
        addCorsHeaders(exchange);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        
        var json = body != null ? JsonSerde.toJson(body) : "";
        var bytes = json.getBytes(StandardCharsets.UTF_8);
        
        var acceptEncoding = exchange.getRequestHeaders().getFirst("Accept-Encoding");
        boolean useGzip = compressionEnabled && acceptEncoding != null && acceptEncoding.contains("gzip");
        
        if (useGzip && bytes.length > 1024) {
            exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            var baos = new java.io.ByteArrayOutputStream();
            try (var gzos = new GZIPOutputStream(baos)) {
                gzos.write(bytes);
            }
            bytes = baos.toByteArray();
        }
        
        exchange.getResponseHeaders().set("Content-Length", String.valueOf(bytes.length));
        exchange.sendResponseHeaders(status, bytes.length);
        try (var os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            if ("GET".equals(exchange.getRequestMethod())) {
                sendJson(exchange, 200, db.metrics().snapshot());
            }
        }
    }

    private class MetricsStreamHandler implements HttpHandler {
        private volatile boolean running = true;

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            
            // SSE headers
            exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.getResponseHeaders().set("Connection", "keep-alive");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            
            exchange.sendResponseHeaders(200, 0);
            
            try (var os = exchange.getResponseBody()) {
                while (running && !Thread.currentThread().isInterrupted()) {
                    try {
                        var metrics = db.metrics().snapshot();
                        var event = "data: " + JsonSerde.toJson(metrics) + "\n\n";
                        os.write(event.getBytes(StandardCharsets.UTF_8));
                        os.flush();
                        Thread.sleep(1000); // Stream metrics every second
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
    }

    private class StatsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
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

    private class BulkHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            if (parts.length < 4) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/bulk/{collection}"));
                return;
            }
            var collectionName = parts[3];
            var collection = db.documentCollection(collectionName);
            
            if ("POST".equals(exchange.getRequestMethod())) {
                var body = readBody(exchange);
                var docs = JsonSerde.fromJson(body, java.util.List.class);
                var count = 0;
                if (docs instanceof java.util.List) {
                    for (Object doc : (java.util.List<?>) docs) {
                        if (doc instanceof java.util.Map) {
                            var docMap = (java.util.Map<?, ?>) doc;
                            var docEntity = new org.junify.db.nosql.document.Document();
                            docEntity.id(java.util.UUID.randomUUID().toString());
                            var fields = new java.util.HashMap<String, Object>();
                            for (var entry : docMap.entrySet()) {
                                fields.put(String.valueOf(entry.getKey()), entry.getValue());
                            }
                            docEntity.getFields().putAll(fields);
                            collection.insert(docEntity);
                            count++;
                        }
                    }
                }
                sendJson(exchange, 201, Map.of(
                    "status", "success",
                    "collection", collectionName,
                    "inserted", count
                ));
            } else if ("DELETE".equals(exchange.getRequestMethod())) {
                var count = 0;
                for (var doc : collection.findAll()) {
                    collection.deleteById(doc.getId());
                    count++;
                }
                sendJson(exchange, 200, Map.of(
                    "status", "success",
                    "collection", collectionName,
                    "deleted", count
                ));
            } else {
                sendJson(exchange, 405, Map.of("error", "Only POST or DELETE allowed"));
            }
        }
    }

    private class SqlHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendJson(exchange, 405, Map.of("error", "Only POST method is allowed for SQL execution"));
                return;
            }
            if (!db.isH2Engine()) {
                sendJson(exchange, 400, Map.of("error", "SQL execution is only available with H2 storage engine"));
                return;
            }
            try {
                var sql = readBody(exchange);
                var result = db.h2Engine().executeSql(sql);
                if (result.success()) {
                    if (result.rows() != null) {
                        sendJson(exchange, 200, Map.of(
                            "type", "select",
                            "columns", result.columns(),
                            "rows", result.rows()
                        ));
                    } else {
                        sendJson(exchange, 200, Map.of(
                            "type", "other",
                            "affected", result.affected(),
                            "message", result.message()
                        ));
                    }
                } else {
                    sendJson(exchange, 400, Map.of("error", result.message()));
                }
            } catch (Exception e) {
                sendJson(exchange, 500, Map.of("error", e.getMessage()));
            }
        }
    }

    private class CDCHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            
            if (parts.length == 3) {
                if ("GET".equals(exchange.getRequestMethod())) {
                    var status = db.cdcManager().getStatus();
                    sendJson(exchange, 200, status);
                    return;
                }
            }
            
            if (parts.length >= 4) {
                var action = parts[3];
                
                if ("connectors".equals(action) && parts.length >= 5) {
                    var connectorName = parts[4];
                    
                    if ("POST".equals(exchange.getRequestMethod())) {
                        var body = readBody(exchange);
                        var data = JsonSerde.fromJson(body, Map.class);
                        var type = data.get("type").toString();
                        
                        if ("file".equals(type)) {
                            var outputDir = java.nio.file.Paths.get(data.get("outputDir").toString());
                            db.cdcManager().addFileConnector(connectorName, outputDir);
                            sendJson(exchange, 201, Map.of("status", "connected", "type", "file", "name", connectorName));
                        } else if ("kafka".equals(type)) {
                            var bootstrapServers = data.get("bootstrapServers").toString();
                            var topic = data.get("topic").toString();
                            db.cdcManager().addKafkaConnector(connectorName, bootstrapServers, topic);
                            sendJson(exchange, 201, Map.of("status", "connected", "type", "kafka", "name", connectorName));
                        } else {
                            sendJson(exchange, 400, Map.of("error", "Unknown connector type"));
                        }
                        return;
                    } else if ("DELETE".equals(exchange.getRequestMethod())) {
                        db.cdcManager().removeFileConnector(connectorName);
                        db.cdcManager().removeKafkaConnector(connectorName);
                        sendJson(exchange, 200, Map.of("status", "disconnected", "name", connectorName));
                        return;
                    }
                }
                
                if ("events".equals(action)) {
                    var since = exchange.getRequestHeaders().getFirst("Since");
                    var events = since != null 
                        ? db.cdcManager().processor().getEventsSince(Long.parseLong(since))
                        : db.cdcManager().processor().getEventLog();
                    sendJson(exchange, 200, Map.of("events", events));
                    return;
                }
                
                if ("enable".equals(action)) {
                    db.cdcManager().processor().enable();
                    sendJson(exchange, 200, Map.of("status", "enabled"));
                    return;
                }
                
                if ("disable".equals(action)) {
                    db.cdcManager().processor().disable();
                    sendJson(exchange, 200, Map.of("status", "disabled"));
                    return;
                }
            }
            
            sendJson(exchange, 400, Map.of("error", "Usage: GET /api/cdc, POST/DELETE /api/cdc/connectors/{name}, GET /api/cdc/events"));
        }
    }

    private class SchemaSqlHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            
            if ("GET".equals(exchange.getRequestMethod()) && path.equals("/api/schema")) {
                var tables = db.h2Engine().schemaManager().getTables();
                sendJson(exchange, 200, Map.of("tables", tables));
                return;
            }
            
            var parts = path.split("/");
            if (parts.length >= 4) {
                var tableName = parts[3];
                var sm = db.h2Engine().schemaManager();
                
                if ("GET".equals(exchange.getRequestMethod())) {
                    var tables = sm.getTables().stream()
                        .filter(t -> t.startsWith(tableName))
                        .map(t -> Map.of("name", t, "columns", sm.getColumns(t)))
                        .collect(Collectors.toList());
                    sendJson(exchange, 200, Map.of("tables", tables));
                    return;
                }
                
                if ("POST".equals(exchange.getRequestMethod())) {
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var columnsRaw = (Map<String, Object>) data.get("columns");
                    var columns = new java.util.HashMap<String, Object>();
                    for (var entry : columnsRaw.entrySet()) {
                        columns.put(entry.getKey(), entry.getValue());
                    }
                    var result = sm.createTable(tableName, columns);
                    sendJson(exchange, result.success() ? 201 : 400, 
                        Map.of("success", result.success(), "message", result.message()));
                    return;
                }
                
                if ("DELETE".equals(exchange.getRequestMethod())) {
                    var result = sm.dropTable(tableName);
                    sendJson(exchange, result.success() ? 200 : 400, 
                        Map.of("success", result.success(), "message", result.message()));
                    return;
                }
            }
            
            sendJson(exchange, 400, Map.of("error", "Usage: GET /api/schema or GET/DELETE /api/schema/{table}"));
        }
    }

    private class TablesHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            
            if (parts.length < 4) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/tables/{name}"));
                return;
            }
            
            var tableName = parts[3];
            var sm = db.h2Engine().schemaManager();
            
            if ("GET".equals(exchange.getRequestMethod())) {
                sendJson(exchange, 200, sm.getTableInfo(tableName));
            } else if ("POST".equals(exchange.getRequestMethod())) {
                var body = readBody(exchange);
                var data = JsonSerde.fromJson(body, Map.class);
                var columnsRaw = (Map<String, Object>) data.get("columns");
                var columns = new java.util.HashMap<String, Object>();
                for (var entry : columnsRaw.entrySet()) {
                    var colDef = (Map<String, Object>) entry.getValue();
                    var colType = colDef.get("type") != null ? colDef.get("type").toString() : "VARCHAR";
                    columns.put(entry.getKey(), colType);
                }
                var result = sm.createTable(tableName, columns);
                sendJson(exchange, result.success() ? 201 : 400, 
                    Map.of("success", result.success(), "message", result.message()));
            } else {
                sendJson(exchange, 405, Map.of("error", "Method not allowed"));
            }
        }
    }

    private class ConstraintsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            
            if (parts.length < 4) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/constraints/{table}"));
                return;
            }
            
            var tableName = parts[3];
            var cm = db.h2Engine().constraintManager();
            
            if ("GET".equals(exchange.getRequestMethod())) {
                var constraints = cm.getAllConstraints(tableName);
                sendJson(exchange, 200, constraints);
            } else {
                sendJson(exchange, 405, Map.of("error", "Method not allowed"));
            }
        }
    }
}
