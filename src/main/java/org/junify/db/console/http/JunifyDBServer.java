package org.junify.db.console.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.document.Query;
import org.junify.db.nosql.document.QueryParser;
import org.junify.db.core.util.JsonSerde;
import org.junify.db.nosql.kv.HashBucket;
import org.junify.db.nosql.kv.ListBucket;
import org.junify.db.nosql.kv.SetBucket;
import org.junify.db.storage.spi.SchemaManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

public class JunifyDBServer {

    private final JunifyDB db;
    private HttpServer server;
    private long startTime;
    private String apiKey = "hYXuECpj4dM28vf3En47ar2KaA1FPMLVNrmAYYSAoFM";  // Default API key - change in production!
    private boolean authEnabled = true;  // Authentication enabled by default for security
    private boolean corsEnabled = true;
    private boolean compressionEnabled = true;
    private int rateLimit = 1000;
    private long maxRequestSizeBytes = 10 * 1024 * 1024; // 10MB default max request size
    private int queryTimeoutSeconds = 30; // Default query timeout
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
        } else {
            // Explicitly disable auth if null/empty is passed (not recommended)
            this.authEnabled = false;
            this.apiKey = null;
        }
    }

    /**
     * Disable authentication (NOT RECOMMENDED for production).
     * Only use in trusted environments.
     */
    public void disableAuthentication() {
        this.authEnabled = false;
        System.err.println("WARNING: Authentication disabled. This is unsafe in production!");
    }

    /**
     * Set maximum request size in bytes.
     * Default is 10MB to prevent OOM attacks.
     */
    public void setMaxRequestSize(long bytes) {
        this.maxRequestSizeBytes = bytes;
    }

    /**
     * Set query timeout in seconds.
     * Default is 30 seconds to prevent hanging queries.
     */
    public void setQueryTimeout(int seconds) {
        this.queryTimeoutSeconds = seconds;
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
        
        // Log security configuration
        if (authEnabled) {
            System.out.println("[JunifyDBServer] Authentication ENABLED with API key");
            System.out.println("[JunifyDBServer] Include header: X-API-Key: <your-key>");
            if (apiKey != null && !apiKey.isEmpty()) {
                // Don't log full key in production - truncated for security
                String maskedKey = apiKey.length() > 8 ? apiKey.substring(0, 8) + "..." : "***";
                System.out.println("[JunifyDBServer] API key prefix: " + maskedKey);
            }
        } else {
            System.err.println("[JunifyDBServer] WARNING: Authentication DISABLED - insecure!");
        }

        server.createContext("/", new StaticHandler());
        server.createContext("/api/collections/", new CollectionsHandler());
        server.createContext("/api/kv/", new KeyValueHandler());
        server.createContext("/api/kv/lists/", new ListHandler());
        server.createContext("/api/kv/sets/", new SetHandler());
        server.createContext("/api/kv/hashes/", new HashHandler());
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
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            
            // /api/collections/ with no additional path - list info
            if (parts.length < 4 || parts[3].isEmpty()) {
                if ("GET".equals(exchange.getRequestMethod())) {
                    sendJson(exchange, 200, Map.of("collections", "use /api/collections/{name}"));
                } else {
                    sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                }
                return;
            }
            
            // /api/collections/{name} - delegate to collection logic
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
                        System.err.println("[CollectionsHandler] POST error: " + e.getMessage());
                        e.printStackTrace();
                        try {
                            sendJson(exchange, 500, Map.of("error", "Internal server error", "message", e.getMessage()));
                        } catch (Exception ex) {
                            // Response already sent or connection closed
                        }
                    }
                } else {
                    sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                }
            } else if (parts.length >= 5) {
                // Check for /api/collections/{name}/stats endpoint
                if ("stats".equals(parts[4])) {
                    if ("GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 200, collection.stats());
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }

                // Check for /api/collections/{name}/set-ttl endpoint
                if ("set-ttl".equals(parts[4])) {
                    if ("POST".equals(exchange.getRequestMethod())) {
                        try {
                            var body = readBody(exchange);
                            var data = JsonSerde.fromJson(body, Map.class);
                            var documentId = data.get("documentId").toString();
                            var ttlSeconds = ((Number) data.get("ttlSeconds")).longValue();
                            var updated = collection.setTtl(documentId, ttlSeconds);
                            sendJson(exchange, 200, Map.of(
                                    "success", updated > 0,
                                    "updated", updated
                            ));
                        } catch (Exception e) {
                            System.err.println("[CollectionsHandler] Set-TTL error: " + e.getMessage());
                            e.printStackTrace();
                            sendJson(exchange, 500, Map.of("error", "Set TTL failed", "message", e.getMessage()));
                        }
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }

                // Check for /api/collections/{name}/cleanup endpoint
                if ("cleanup".equals(parts[4])) {
                    if ("POST".equals(exchange.getRequestMethod())) {
                        try {
                            var deleted = collection.cleanupExpired();
                            sendJson(exchange, 200, Map.of("deleted", deleted));
                        } catch (Exception e) {
                            System.err.println("[CollectionsHandler] Cleanup error: " + e.getMessage());
                            e.printStackTrace();
                            sendJson(exchange, 500, Map.of("error", "Cleanup failed", "message", e.getMessage()));
                        }
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }

                // Check for /api/collections/{name}/query endpoint
                if ("query".equals(parts[4])) {
                    if ("POST".equals(exchange.getRequestMethod())) {
                        try {
                            var body = readBody(exchange);
                            var data = JsonSerde.fromJson(body, Map.class);

                            // Simple query format: {"field": "value"} for equality
                            // Or: {"$gt": {"field": 30}} for greater than
                            // Or: {"$lt": {"field": 50}} for less than
                            org.junify.db.nosql.document.Query query = null;

                            if (data.containsKey("$gt")) {
                                var gtData = (Map<String, Object>) data.get("$gt");
                                for (var entry : gtData.entrySet()) {
                                    query = org.junify.db.nosql.document.Query.gt(entry.getKey(), ((Number) entry.getValue()).doubleValue());
                                }
                            } else if (data.containsKey("$lt")) {
                                var ltData = (Map<String, Object>) data.get("$lt");
                                for (var entry : ltData.entrySet()) {
                                    query = org.junify.db.nosql.document.Query.lt(entry.getKey(), ((Number) entry.getValue()).doubleValue());
                                }
                            } else if (data.containsKey("$eq")) {
                                var eqData = (Map<String, Object>) data.get("$eq");
                                for (Object entryObj : eqData.entrySet()) {
                                    var entry = (java.util.Map.Entry<String, Object>) entryObj;
                                    query = org.junify.db.nosql.document.Query.eq(entry.getKey(), entry.getValue());
                                }
                            } else {
                                // Default: simple equality query
                                for (Object entryObj : data.entrySet()) {
                                    var entry = (java.util.Map.Entry<String, Object>) entryObj;
                                    query = org.junify.db.nosql.document.Query.eq(entry.getKey(), entry.getValue());
                                    break; // Only first field for simple query
                                }
                            }

                            if (query == null) {
                                query = org.junify.db.nosql.document.Query.all();
                            }

                            var results = collection.find(query);
                            sendJson(exchange, 200, results.stream()
                                .map(Document::getFields)
                                .collect(java.util.stream.Collectors.toList()));
                        } catch (Exception e) {
                            System.err.println("[CollectionsHandler] Query error: " + e.getMessage());
                            e.printStackTrace();
                            sendJson(exchange, 500, Map.of("error", "Query failed", "message", e.getMessage()));
                        }
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }
                
                var id = parts[4];
                System.out.println("[CollectionsHandler] " + exchange.getRequestMethod() + " /api/collections/" + name + "/" + id);
                if ("GET".equals(exchange.getRequestMethod())) {
                    var doc = collection.findById(id);
                    System.out.println("[CollectionsHandler] GET result: " + (doc != null ? "found" : "not found"));
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
                        System.err.println("[CollectionsHandler] PUT/POST error: " + e.getMessage());
                        e.printStackTrace();
                        try {
                            sendJson(exchange, 500, Map.of("error", "Internal server error", "message", e.getMessage()));
                        } catch (Exception ex) {
                            // Response already sent or connection closed
                        }
                    }
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    try {
                        boolean deleted = collection.deleteById(id);
                        if (deleted) {
                            sendJson(exchange, 204, null);
                        } else {
                            sendJson(exchange, 404, Map.of("error", "Not found", "id", id));
                        }
                    } catch (Exception e) {
                        System.err.println("[CollectionsHandler] DELETE error: " + e.getMessage());
                        e.printStackTrace();
                        try {
                            sendJson(exchange, 500, Map.of("error", "Delete failed", "message", e.getMessage()));
                        } catch (Exception ex) {
                            // Response already sent or connection closed
                        }
                    }
                } else {
                    sendJson(exchange, 405, Map.of("error", "Method not allowed"));
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
                    var value = data.getOrDefault("value", "").toString();
                    bucket.put(key, value);
                    sendJson(exchange, 201, Map.of("key", key, "value", value, "status", "created"));
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    bucket.delete(key);
                    sendJson(exchange, 204, null);
                }
            } else {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/kv/{bucket}/{key}"));
            }
        }
    }

    private class ListHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            
            // Context path /api/kv/lists/ is stripped by HttpServer
            // Remaining path: /{bucket}/{key}[/{operation}]
            // parts[0]="", [1]="bucket", [2]="key", [3]="operation"
            if (parts.length < 3) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/kv/lists/{bucket}/{key}[/{operation}]"));
                return;
            }
            
            var bucketName = parts[1];
            var bucket = db.listBucket(bucketName);
            var key = parts[2];
            
            // If only bucket and key (no operation), return full list
            if (parts.length == 3 || (parts.length == 4 && parts[3].isEmpty())) {
                if ("GET".equals(exchange.getRequestMethod())) {
                    var result = bucket.lrange(key, 0, -1);
                    sendJson(exchange, 200, Map.of("key", key, "values", result, "length", result.size()));
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    boolean deleted = bucket.delete(key);
                    sendJson(exchange, deleted ? 204 : 404, Map.of("deleted", deleted));
                } else {
                    sendJson(exchange, 400, Map.of("error", "Usage: GET or DELETE /api/kv/lists/{bucket}/{key}"));
                }
                return;
            }
            
            // /api/kv/lists/{bucket}/{key}/{operation}
            var operation = parts[3];
            
            switch (operation.toLowerCase()) {
                case "lpush" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var values = parseStringArray(data.get("values"));
                    long len = bucket.lpush(key, values);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "lpush", "length", len));
                }
                case "rpush" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var values = parseStringArray(data.get("values"));
                    long len = bucket.rpush(key, values);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "rpush", "length", len));
                }
                case "lpop" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    String value = bucket.lpop(key);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "lpop", "value", value));
                }
                case "rpop" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    String value = bucket.rpop(key);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "rpop", "value", value));
                }
                case "range", "lrange" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    var query = exchange.getRequestURI().getQuery();
                    int start = 0, end = -1;
                    if (query != null) {
                        for (String param : query.split("&")) {
                            var kv = param.split("=");
                            if (kv.length == 2) {
                                if ("start".equals(kv[0])) start = Integer.parseInt(kv[1]);
                                if ("end".equals(kv[0])) end = Integer.parseInt(kv[1]);
                            }
                        }
                    }
                    var result = bucket.lrange(key, start, end);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "lrange", "start", start, "end", end, "values", result));
                }
                case "len", "llen" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    long len = bucket.llen(key);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "llen", "length", len));
                }
                case "lrem" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    long count = data.containsKey("count") ? ((Number) data.get("count")).longValue() : 0;
                    String value = data.get("value").toString();
                    long removed = bucket.lrem(key, count, value);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "lrem", "removed", removed));
                }
                case "lindex" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    var query = exchange.getRequestURI().getQuery();
                    int index = 0;
                    if (query != null) {
                        for (String param : query.split("&")) {
                            var kv = param.split("=");
                            if (kv.length == 2 && "index".equals(kv[0])) {
                                index = Integer.parseInt(kv[1]);
                            }
                        }
                    }
                    String value = bucket.lindex(key, index);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "lindex", "index", index, "value", value));
                }
                case "ltrim" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    int start = ((Number) data.get("start")).intValue();
                    int end = ((Number) data.get("end")).intValue();
                    bucket.ltrim(key, start, end);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "ltrim", "start", start, "end", end));
                }
                case "stats" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    sendJson(exchange, 200, bucket.stats());
                }
                default -> sendJson(exchange, 400, Map.of("error", "Unknown operation: " + operation, 
                    "supported", "lpush, rpush, lpop, rpop, range, len, lrem, lindex, ltrim, stats"));
            }
        }
    }

    private class SetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            
            // Context path /api/kv/sets/ is stripped by HttpServer
            // Remaining path: /{bucket}/{key}[/{operation}]
            // parts[0]="", [1]="bucket", [2]="key", [3]="operation"
            if (parts.length < 3) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/kv/sets/{bucket}/{key}[/{operation}]"));
                return;
            }
            
            var bucketName = parts[1];
            var bucket = db.setBucket(bucketName);
            var key = parts[2];
            
            // If only bucket and key (no operation), return all members
            if (parts.length == 3 || (parts.length == 4 && parts[3].isEmpty())) {
                if ("GET".equals(exchange.getRequestMethod())) {
                    var members = bucket.smembers(key);
                    sendJson(exchange, 200, Map.of("key", key, "members", members, "cardinality", members.size()));
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    boolean deleted = bucket.delete(key);
                    sendJson(exchange, deleted ? 204 : 404, Map.of("deleted", deleted));
                } else {
                    sendJson(exchange, 400, Map.of("error", "Usage: GET or DELETE /api/kv/sets/{bucket}/{key}"));
                }
                return;
            }
            
            // /api/kv/sets/{bucket}/{key}/{operation}
            var operation = parts[3];
            
            switch (operation.toLowerCase()) {
                case "sadd" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var members = parseStringArray(data.get("members"));
                    long added = bucket.sadd(key, members);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "sadd", "added", added));
                }
                case "srem" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var members = parseStringArray(data.get("members"));
                    long removed = bucket.srem(key, members);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "srem", "removed", removed));
                }
                case "smembers" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    var members = bucket.smembers(key);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "smembers", "members", members));
                }
                case "sismember", "contains" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    var query = exchange.getRequestURI().getQuery();
                    String member = null;
                    if (query != null) {
                        for (String param : query.split("&")) {
                            var kv = param.split("=");
                            if (kv.length == 2 && "member".equals(kv[0])) {
                                member = java.net.URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
                            }
                        }
                    }
                    if (member == null) {
                        sendJson(exchange, 400, Map.of("error", "Missing 'member' query parameter"));
                        return;
                    }
                    boolean exists = bucket.sismember(key, member);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "sismember", "member", member, "exists", exists));
                }
                case "scard", "card" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    long card = bucket.scard(key);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "scard", "cardinality", card));
                }
                case "spop" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    int count = data.containsKey("count") ? ((Number) data.get("count")).intValue() : 1;
                    if (count == 1) {
                        String member = bucket.spop(key);
                        sendJson(exchange, 200, Map.of("key", key, "operation", "spop", "member", member));
                    } else {
                        var members = bucket.spop(key, count);
                        sendJson(exchange, 200, Map.of("key", key, "operation", "spop", "members", members, "count", members.size()));
                    }
                }
                case "srandmember", "randmember" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    var query = exchange.getRequestURI().getQuery();
                    int count = 1;
                    if (query != null) {
                        for (String param : query.split("&")) {
                            var kv = param.split("=");
                            if (kv.length == 2 && "count".equals(kv[0])) {
                                count = Integer.parseInt(kv[1]);
                            }
                        }
                    }
                    if (count == 1) {
                        String member = bucket.srandmember(key);
                        sendJson(exchange, 200, Map.of("key", key, "operation", "srandmember", "member", member));
                    } else {
                        var members = bucket.srandmember(key, count);
                        sendJson(exchange, 200, Map.of("key", key, "operation", "srandmember", "members", members));
                    }
                }
                case "sinter", "inter" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var keys = parseStringArray(data.get("keys"));
                    var result = bucket.sinter(keys);
                    sendJson(exchange, 200, Map.of("operation", "sinter", "keys", java.util.Arrays.toString(keys), "intersection", result));
                }
                case "sunion", "union" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var keys = parseStringArray(data.get("keys"));
                    var result = bucket.sunion(keys);
                    sendJson(exchange, 200, Map.of("operation", "sunion", "keys", java.util.Arrays.toString(keys), "union", result));
                }
                case "sdiff", "diff" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var keys = parseStringArray(data.get("keys"));
                    var result = bucket.sdiff(keys);
                    sendJson(exchange, 200, Map.of("operation", "sdiff", "keys", java.util.Arrays.toString(keys), "difference", result));
                }
                case "stats" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    sendJson(exchange, 200, bucket.stats());
                }
                default -> sendJson(exchange, 400, Map.of("error", "Unknown operation: " + operation,
                    "supported", "sadd, srem, smembers, sismember, scard, spop, srandmember, sinter, sunion, sdiff, stats"));
            }
        }
    }

    private class HashHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            
            // Context path /api/kv/hashes/ is stripped by HttpServer
            // Remaining path: /{bucket}/{key}[/{operation}]
            // parts[0]="", [1]="bucket", [2]="key", [3]="operation"
            if (parts.length < 3) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/kv/hashes/{bucket}/{key}[/{operation}]"));
                return;
            }
            
            var bucketName = parts[1];
            var bucket = db.hashBucket(bucketName);
            var key = parts[2];
            
            // If only bucket and key (no operation), return all fields
            if (parts.length == 3 || (parts.length == 4 && parts[3].isEmpty())) {
                if ("GET".equals(exchange.getRequestMethod())) {
                    var fields = bucket.hgetall(key);
                    sendJson(exchange, 200, Map.of("key", key, "fields", fields, "length", fields.size()));
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    boolean deleted = bucket.delete(key);
                    sendJson(exchange, deleted ? 204 : 404, Map.of("deleted", deleted));
                } else {
                    sendJson(exchange, 400, Map.of("error", "Usage: GET or DELETE /api/kv/hashes/{bucket}/{key}"));
                }
                return;
            }
            
            // /api/kv/hashes/{bucket}/{key}/{operation}
            var operation = parts[3];
            
            switch (operation.toLowerCase()) {
                case "hset", "set" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    if (data.containsKey("field") && data.containsKey("value")) {
                        int result = bucket.hset(key, data.get("field").toString(), data.get("value").toString());
                        sendJson(exchange, 200, Map.of("key", key, "operation", "hset", "field", data.get("field"), "added", result == 1));
                    } else if (data.containsKey("fields")) {
                        @SuppressWarnings("unchecked")
                        var fields = (Map<String, String>) data.get("fields");
                        int added = bucket.hset(key, fields);
                        sendJson(exchange, 200, Map.of("key", key, "operation", "hset", "fieldsAdded", added));
                    } else {
                        sendJson(exchange, 400, Map.of("error", "Missing 'field'/'value' or 'fields' in body"));
                    }
                }
                case "hget", "get" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    var query = exchange.getRequestURI().getQuery();
                    String field = null;
                    if (query != null) {
                        for (String param : query.split("&")) {
                            var kv = param.split("=");
                            if (kv.length == 2 && "field".equals(kv[0])) {
                                field = java.net.URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
                            }
                        }
                    }
                    if (field == null) {
                        sendJson(exchange, 400, Map.of("error", "Missing 'field' query parameter"));
                        return;
                    }
                    String value = bucket.hget(key, field);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hget", "field", field, "value", value));
                }
                case "hgetall", "getall" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    var fields = bucket.hgetall(key);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hgetall", "fields", fields));
                }
                case "hdel", "del" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var fields = parseStringArray(data.get("fields"));
                    int deleted = bucket.hdel(key, fields);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hdel", "deleted", deleted));
                }
                case "hlen", "len" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    long len = bucket.hlen(key);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hlen", "length", len));
                }
                case "hexists", "exists" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    var query = exchange.getRequestURI().getQuery();
                    String field = null;
                    if (query != null) {
                        for (String param : query.split("&")) {
                            var kv = param.split("=");
                            if (kv.length == 2 && "field".equals(kv[0])) {
                                field = java.net.URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
                            }
                        }
                    }
                    if (field == null) {
                        sendJson(exchange, 400, Map.of("error", "Missing 'field' query parameter"));
                        return;
                    }
                    boolean exists = bucket.hexists(key, field);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hexists", "field", field, "exists", exists));
                }
                case "hkeys", "keys" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    var fields = bucket.hkeys(key);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hkeys", "fields", fields));
                }
                case "hvals", "vals" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    var values = bucket.hvals(key);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hvals", "values", values));
                }
                case "hmget", "mget" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var fields = parseStringArray(data.get("fields"));
                    var result = bucket.hmget(key, fields);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hmget", "fields", result));
                }
                case "hincrby", "incrby" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    String field = data.get("field").toString();
                    long delta = data.containsKey("delta") ? ((Number) data.get("delta")).longValue() : 1;
                    long newValue = bucket.hincrby(key, field, delta);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hincrby", "field", field, "newValue", newValue));
                }
                case "hincrbyfloat", "incrbyfloat" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    String field = data.get("field").toString();
                    double delta = data.containsKey("delta") ? ((Number) data.get("delta")).doubleValue() : 1.0;
                    String newValue = bucket.hincrbyfloat(key, field, delta);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hincrbyfloat", "field", field, "newValue", newValue));
                }
                case "stats" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    sendJson(exchange, 200, bucket.stats());
                }
                default -> sendJson(exchange, 400, Map.of("error", "Unknown operation: " + operation,
                    "supported", "hset, hget, hgetall, hdel, hlen, hexists, hkeys, hvals, hmget, hincrby, hincrbyfloat, stats"));
            }
        }
    }

    private String[] parseStringArray(Object obj) {
        if (obj == null) return new String[0];
        if (obj instanceof String str) {
            return new String[]{str};
        }
        if (obj instanceof java.util.List<?> list) {
            return list.stream().map(Object::toString).toArray(String[]::new);
        }
        return new String[0];
    }

    private class ColumnHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }
            var path = exchange.getRequestURI().getPath();
            var parts = path.split("/");
            var query = exchange.getRequestURI().getQuery();

            // /api/columns/{family}
            if (parts.length < 4 || parts[3].isEmpty()) {
                sendJson(exchange, 400, Map.of("error", "Usage: /api/columns/{family}[/{row}][/operation]"));
                return;
            }

            var familyName = parts[3];
            var cf = db.columnFamily(familyName);

            // /api/columns/{family}/stats
            if (parts.length == 4 && "stats".equals(parts[3])) {
                // This case won't happen due to above check, handled below
            }

            // /api/columns/{family}/stats - Family-level stats
            if (parts.length >= 5 && "stats".equals(parts[4])) {
                if ("GET".equals(exchange.getRequestMethod())) {
                    sendJson(exchange, 200, cf.getColumnStats());
                } else {
                    sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                }
                return;
            }

            // /api/columns/{family}/cleanup - Cleanup expired columns
            if (parts.length >= 5 && "cleanup".equals(parts[4])) {
                if ("POST".equals(exchange.getRequestMethod())) {
                    var deleted = cf.cleanupAllExpired();
                    sendJson(exchange, 200, Map.of("deleted", deleted));
                } else {
                    sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                }
                return;
            }

            // /api/columns/{family}/{row}
            if (parts.length >= 5) {
                var rowKey = parts[4];

                // /api/columns/{family}/{row}/stats - Row-level stats
                if (parts.length >= 6 && "stats".equals(parts[5])) {
                    if ("GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 200, cf.getRowStats(rowKey));
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }

                // /api/columns/{family}/{row}/get-range - Paginated row retrieval
                if (parts.length >= 6 && "get-range".equals(parts[5])) {
                    if ("GET".equals(exchange.getRequestMethod())) {
                        var params = parseQueryParams(query);
                        int limit = params.containsKey("limit") ? Integer.parseInt(params.get("limit")) : 100;
                        int offset = params.containsKey("offset") ? Integer.parseInt(params.get("offset")) : 0;
                        var columnsParam = params.get("columns");
                        Set<String> columns = null;
                        if (columnsParam != null && !columnsParam.isEmpty()) {
                            columns = new HashSet<>(Arrays.asList(columnsParam.split(",")));
                        }
                        var data = cf.getRow(rowKey, limit, offset, columns);
                        sendJson(exchange, 200, Map.of(
                            "rowKey", rowKey,
                            "limit", limit,
                            "offset", offset,
                            "columnsReturned", data.size(),
                            "data", data
                        ));
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }

                // /api/columns/{family}/{row}/filter - Filter columns by name
                if (parts.length >= 6 && "filter".equals(parts[5])) {
                    if ("GET".equals(exchange.getRequestMethod())) {
                        var params = parseQueryParams(query);
                        var columnsParam = params.get("columns");
                        if (columnsParam != null && !columnsParam.isEmpty()) {
                            var columns = new HashSet<String>(Arrays.asList(columnsParam.split(",")));
                            var data = cf.getRow(rowKey, columns);
                            sendJson(exchange, 200, Map.of("rowKey", rowKey, "data", data));
                        } else {
                            sendJson(exchange, 400, Map.of("error", "Missing 'columns' query parameter"));
                        }
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }

                // /api/columns/{family}/{row}/filter-pattern - Filter by regex pattern
                if (parts.length >= 6 && "filter-pattern".equals(parts[5])) {
                    if ("GET".equals(exchange.getRequestMethod())) {
                        var params = parseQueryParams(query);
                        var pattern = params.get("pattern");
                        if (pattern != null && !pattern.isEmpty()) {
                            var data = cf.getRowByPattern(rowKey, pattern);
                            sendJson(exchange, 200, Map.of("rowKey", rowKey, "pattern", pattern, "data", data));
                        } else {
                            sendJson(exchange, 400, Map.of("error", "Missing 'pattern' query parameter"));
                        }
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }

                // /api/columns/{family}/{row}/filter-prefix - Filter by prefix
                if (parts.length >= 6 && "filter-prefix".equals(parts[5])) {
                    if ("GET".equals(exchange.getRequestMethod())) {
                        var params = parseQueryParams(query);
                        var prefix = params.get("prefix");
                        if (prefix != null && !prefix.isEmpty()) {
                            var data = cf.getRowByPrefix(rowKey, prefix);
                            sendJson(exchange, 200, Map.of("rowKey", rowKey, "prefix", prefix, "data", data));
                        } else {
                            sendJson(exchange, 400, Map.of("error", "Missing 'prefix' query parameter"));
                        }
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }

                // /api/columns/{family}/{row}/ttl/{column} - Get TTL for a column
                if (parts.length >= 7 && "ttl".equals(parts[5])) {
                    var column = parts[6];
                    if ("GET".equals(exchange.getRequestMethod())) {
                        var remainingTtl = cf.getRemainingTtl(rowKey, column);
                        var columnData = cf.getColumnData(rowKey, column);
                        if (columnData == null) {
                            sendJson(exchange, 404, Map.of("error", "Column not found"));
                        } else {
                            sendJson(exchange, 200, Map.of(
                                "rowKey", rowKey,
                                "column", column,
                                "remainingTtlSeconds", remainingTtl,
                                "hasTtl", columnData.hasTtl(),
                                "expiresAt", columnData.getExpiresAt()
                            ));
                        }
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }

                // /api/columns/{family}/{row}/column/{column} - Single column operations
                if (parts.length >= 7 && "column".equals(parts[5])) {
                    var column = parts[6];
                    if ("GET".equals(exchange.getRequestMethod())) {
                        var value = cf.get(rowKey, column);
                        if (value != null) {
                            var columnData = cf.getColumnData(rowKey, column);
                            sendJson(exchange, 200, Map.of(
                                "rowKey", rowKey,
                                "column", column,
                                "value", value,
                                "hasTtl", columnData != null && columnData.hasTtl(),
                                "remainingTtlSeconds", columnData != null ? columnData.getRemainingTtlSeconds() : -1
                            ));
                        } else {
                            sendJson(exchange, 404, Map.of("error", "Column not found"));
                        }
                    } else if ("PUT".equals(exchange.getRequestMethod()) || "POST".equals(exchange.getRequestMethod())) {
                        var body = readBody(exchange);
                        var data = JsonSerde.fromJson(body, Map.class);
                        var value = data.get("value");
                        Integer ttl = data.containsKey("ttlSeconds") ? ((Number) data.get("ttlSeconds")).intValue() : null;
                        cf.put(rowKey, column, value, ttl);
                        sendJson(exchange, 201, Map.of("rowKey", rowKey, "column", column, "status", "created"));
                    } else if ("DELETE".equals(exchange.getRequestMethod())) {
                        cf.deleteColumn(rowKey, column);
                        sendJson(exchange, 204, null);
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }

                // /api/columns/{family}/{row}/cleanup - Cleanup expired columns in row
                if (parts.length >= 6 && "cleanup".equals(parts[5])) {
                    if ("POST".equals(exchange.getRequestMethod())) {
                        var deleted = cf.cleanupExpiredColumns(rowKey);
                        sendJson(exchange, 200, Map.of("deleted", deleted));
                    } else {
                        sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                    }
                    return;
                }

                // Default: Full row operations
                if ("GET".equals(exchange.getRequestMethod())) {
                    var row = cf.getRow(rowKey);
                    if (row != null && !row.isEmpty()) {
                        sendJson(exchange, 200, Map.of("rowKey", rowKey, "columns", row));
                    } else {
                        sendJson(exchange, 404, Map.of("error", "Row not found"));
                    }
                } else if ("PUT".equals(exchange.getRequestMethod()) || "POST".equals(exchange.getRequestMethod())) {
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    for (Object o : data.entrySet()) {
                        var entry = (java.util.Map.Entry<?, ?>) o;
                        var value = entry.getValue();
                        Integer ttl = null;
                        // Support nested TTL format: {"column": {"value": "x", "ttlSeconds": 60}}
                        if (value instanceof Map) {
                            var valueMap = (Map<?, ?>) value;
                            value = valueMap.get("value");
                            if (valueMap.containsKey("ttlSeconds")) {
                                ttl = ((Number) valueMap.get("ttlSeconds")).intValue();
                            }
                        }
                        cf.put(rowKey, entry.getKey().toString(), value, ttl);
                    }
                    sendJson(exchange, 201, Map.of("rowKey", rowKey, "status", "created"));
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    cf.deleteRow(rowKey);
                    sendJson(exchange, 204, null);
                } else {
                    sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                }
            } else {
                // /api/columns/{family} - List all row keys
                if ("GET".equals(exchange.getRequestMethod())) {
                    var rowKeys = cf.getRowKeys();
                    sendJson(exchange, 200, Map.of(
                        "family", familyName,
                        "rowCount", rowKeys.size(),
                        "rowKeys", rowKeys
                    ));
                } else {
                    sendJson(exchange, 400, Map.of("error", "Usage: /api/columns/{family}[/{row}][/operation]"));
                }
            }
        }

        private Map<String, String> parseQueryParams(String query) {
            var params = new LinkedHashMap<String, String>();
            if (query != null) {
                for (var param : query.split("&")) {
                    var kv = param.split("=", 2);
                    if (kv.length == 2) {
                        params.put(kv[0], java.net.URLDecoder.decode(kv[1], java.nio.charset.StandardCharsets.UTF_8));
                    } else if (kv.length == 1) {
                        params.put(kv[0], "");
                    }
                }
            }
            return params;
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
            // /api/schema/ with no collection - return full schema info
            if (parts.length < 4 || parts[3].isEmpty()) {
                if ("GET".equals(exchange.getRequestMethod())) {
                    var h2Engine = db.h2Engine();
                    if (h2Engine != null) {
                        var schemaInfo = h2Engine.schemaManager().getSchemaInfo();
                        sendJson(exchange, 200, schemaInfo);
                    } else {
                        sendJson(exchange, 500, Map.of("error", "H2 engine not available"));
                    }
                } else {
                    sendJson(exchange, 405, Map.of("error", "Method not allowed"));
                }
                return;
            }
            
            var collectionName = parts[3];

            if ("GET".equals(exchange.getRequestMethod())) {
                // Check if it's a specific table schema request
                var h2Engine = db.h2Engine();
                if (h2Engine == null) {
                    sendJson(exchange, 500, Map.of("error", "H2 engine not available"));
                    return;
                }
                var schemaInfo = h2Engine.schemaManager().getSchemaInfo();
                @SuppressWarnings("unchecked")
                var tables = (java.util.List<Map<String, Object>>) schemaInfo.get("tables");
                var tableInfo = tables.stream()
                    .filter(t -> collectionName.equals(t.get("name")))
                    .findFirst()
                    .orElse(null);
                
                if (tableInfo != null) {
                    sendJson(exchange, 200, tableInfo);
                } else {
                    sendJson(exchange, 404, Map.of("error", "Table not found: " + collectionName));
                }
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
        // Handle 204 No Content separately
        if (status == 204) {
            addCorsHeaders(exchange);
            exchange.getResponseHeaders().set("Content-Length", "0");
            exchange.sendResponseHeaders(204, -1);
            return;
        }
        
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
        // Check Content-Length header first
        var contentLength = exchange.getRequestHeaders().getFirst("Content-Length");
        if (contentLength != null) {
            var length = Long.parseLong(contentLength);
            if (length > maxRequestSizeBytes) {
                throw new IOException("Request size " + length + " exceeds maximum allowed size " + maxRequestSizeBytes);
            }
        }
        
        // Read body with size limit enforcement
        try (InputStream is = exchange.getRequestBody()) {
            var bytes = is.readAllBytes();
            if (bytes.length > maxRequestSizeBytes) {
                throw new IOException("Request body size " + bytes.length + " exceeds maximum allowed size " + maxRequestSizeBytes);
            }
            return new String(bytes, StandardCharsets.UTF_8);
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
                // Set query timeout before execution
                db.h2Engine().setQueryTimeout(queryTimeoutSeconds);
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
