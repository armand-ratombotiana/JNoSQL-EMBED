"""
Comprehensive patch for R1 (TLS/HTTPS) and R2 (Audit Logging) in JunifyDBServer.java
"""

file_path = r'C:\Users\jratombo-adm\Desktop\JNoSQL-EMBED\src\main\java\org\junify\db\console\http\JunifyDBServer.java'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# R1: Add HTTPS imports
https_imports = '''import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;'''

content = content.replace(
    'import com.sun.net.httpserver.HttpServer;',
    'import com.sun.net.httpserver.HttpServer;\n' + https_imports
)

# R1: Add SSLContext import
content = content.replace(
    'import java.io.IOException;',
    'import java.io.IOException;\nimport javax.net.ssl.SSLContext;'
)

# R2: Add SLF4J imports
content = content.replace(
    'package org.junify.db.console.http;',
    '''package org.junify.db.console.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;'''
)

# R1: Add SSL fields
content = content.replace(
    'private final JunifyDB db;',
    '''private final JunifyDB db;
    private HttpsServer httpsServer;
    private int sslPort = -1;
    private String sslKeystorePath = null;
    private String sslKeystorePassword = null;'''
)

# R2: Add logger and audit log
content = content.replace(
    'private Map<String, RateLimitEntry> rateLimitMap = new ConcurrentHashMap<>();',
    '''private Map<String, RateLimitEntry> rateLimitMap = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(JunifyDBServer.class);
    private final java.util.List<AuditEvent> auditLog = new java.util.concurrent.CopyOnWriteArrayList<>();'''
)

# R2: Add AuditEvent record after RateLimitEntry
content = content.replace(
    '''    private static class RateLimitEntry {
        AtomicInteger count = new AtomicInteger(0);
        long windowStart = System.currentTimeMillis();
    }''',
    '''    private static class RateLimitEntry {
        AtomicInteger count = new AtomicInteger(0);
        long windowStart = System.currentTimeMillis();
    }

    public record AuditEvent(long timestamp, String operation, String resource, String documentId,
                             String status, String clientIp, String details) {}'''
)

# R1: Add SSL configuration methods after setQueryTimeout
ssl_methods = '''
    /**
     * Configure SSL/HTTPS support.
     * @param port SSL port number
     * @param keystorePath Path to JKS keystore file
     * @param keystorePassword Keystore password
     */
    public void configureSsl(int port, String keystorePath, String keystorePassword) {
        this.sslPort = port;
        this.sslKeystorePath = keystorePath;
        this.sslKeystorePassword = keystorePassword;
    }

    public int getSslPort() {
        return sslPort;
    }

    public String getSslKeystorePath() {
        return sslKeystorePath;
    }

    private void logAuditEvent(String operation, String resource, String documentId, String status,
                               String clientIp, String details) {
        var event = new AuditEvent(System.currentTimeMillis(), operation, resource, documentId, status, clientIp, details);
        auditLog.add(event);
        logger.info("[AUDIT] {} {} {} - {} - {} - {}", operation, resource,
                    documentId != null ? documentId : "", status, clientIp, details);
    }

    private void logCrudEvent(String operation, String collection, String documentId, String clientIp) {
        logAuditEvent(operation, collection, documentId, "SUCCESS", clientIp, "CRUD operation");
    }

    private java.util.Map<String, String> parseQueryParams(String query) {
        var params = new java.util.LinkedHashMap<String, String>();
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
'''

content = content.replace(
    '''public void setQueryTimeout(int seconds) {
        this.queryTimeoutSeconds = seconds;
    }''',
    '''public void setQueryTimeout(int seconds) {
        this.queryTimeoutSeconds = seconds;
    }''' + ssl_methods
)

# R1: Add HTTPS startup after server.start()
content = content.replace(
    'server.start();',
    '''server.start();
        // Start HTTPS server if SSL is configured
        if (sslPort > 0 && sslKeystorePath != null) {
            startHttpsServer();
        }'''
)

# R1: Add startHttpsServer method before stop()
https_server_method = '''
    private void startHttpsServer() {
        try {
            System.setProperty("javax.net.ssl.keyStore", sslKeystorePath);
            System.setProperty("javax.net.ssl.keyStorePassword", sslKeystorePassword);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, null, null);

            httpsServer = HttpsServer.create(new InetSocketAddress(sslPort), 0);
            httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
                @Override
                public void configure(HttpsParameters params) {
                    try {
                        SSLContext context = getSSLContext();
                        params.setNeedClientAuth(false);
                        params.setCipherSuites(new String[]{"TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"});
                        params.setProtocols(new String[]{"TLSv1.2", "TLSv1.3"});
                        params.setSSLParameters(context.getDefaultSSLParameters());
                    } catch (Exception e) {
                        System.err.println("[JunifyDBServer] SSL configuration error: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            });

            registerHandlers(httpsServer);
            httpsServer.setExecutor(null);
            httpsServer.start();
            System.out.println("[JunifyDBServer] HTTPS server started on port " + sslPort);
        } catch (Exception e) {
            System.err.println("[JunifyDBServer] SSL initialization error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void registerHandlers(HttpServer server) {
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
        server.createContext("/api/audit/logs", new AuditLogHandler());
        if (corsEnabled) {
            server.createContext("/api/cors", new CorsPreflightHandler());
        }
    }
'''

content = content.replace(
    '''public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }''',
    https_server_method + '''public void stop() {
        if (server != null) {
            server.stop(0);
        }
        if (httpsServer != null) {
            httpsServer.stop(0);
        }
    }'''
)

# R2: Add AuditLogHandler class before StaticHandler
audit_handler = '''
    private class AuditLogHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthValid(exchange)) { sendAuthError(exchange); return; }

            if ("GET".equals(exchange.getRequestMethod())) {
                var query = exchange.getRequestURI().getQuery();
                var params = parseQueryParams(query);

                String operation = params.get("operation");
                String resource = params.get("resource");
                String since = params.get("since");
                int limit = params.containsKey("limit") ? Integer.parseInt(params.get("limit")) : 100;

                var filtered = auditLog.stream();

                if (operation != null && !operation.isEmpty()) {
                    filtered = filtered.filter(e -> e.operation().equals(operation));
                }
                if (resource != null && !resource.isEmpty()) {
                    filtered = filtered.filter(e -> e.resource().equals(resource));
                }
                if (since != null && !since.isEmpty()) {
                    try {
                        long sinceTs = Long.parseLong(since);
                        filtered = filtered.filter(e -> e.timestamp() >= sinceTs);
                    } catch (NumberFormatException ex) {
                        // Ignore invalid since parameter
                    }
                }

                var result = filtered.limit(limit).toList();
                sendJson(exchange, 200, java.util.Map.of(
                    "count", result.size(),
                    "events", result.stream()
                        .map(e -> java.util.Map.of(
                            "timestamp", e.timestamp(),
                            "operation", e.operation(),
                            "resource", e.resource(),
                            "documentId", e.documentId(),
                            "status", e.status(),
                            "clientIp", e.clientIp(),
                            "details", e.details()
                        ))
                        .toList()
                ));
            } else {
                sendJson(exchange, 405, java.util.Map.of("error", "Method not allowed"));
            }
        }
    }

'''

content = content.replace(
    'private class StaticHandler implements HttpHandler {',
    audit_handler + 'private class StaticHandler implements HttpHandler {'
)

# R2: Add audit logging to CollectionsHandler INSERT
content = content.replace(
    '''var doc = Document.fromJson(body);
                        var saved = collection.insert(doc);
                        sendJson(exchange, 201, saved);''',
    '''var doc = Document.fromJson(body);
                        var saved = collection.insert(doc);
                        logCrudEvent("INSERT", name, saved.getId(), getClientIp(exchange));
                        sendJson(exchange, 201, saved);''',
    1  # Only replace first occurrence
)

# R2: Add audit logging to CollectionsHandler UPDATE
content = content.replace(
    '''doc.add(e.getKey().toString(), e.getValue());
                        }
                        var saved = collection.insert(doc);
                        sendJson(exchange, 201, saved);''',
    '''doc.add(e.getKey().toString(), e.getValue());
                        }
                        var saved = collection.insert(doc);
                        logCrudEvent("UPDATE", name, id, getClientIp(exchange));
                        sendJson(exchange, 201, saved);'''
)

# R2: Add audit logging to CollectionsHandler DELETE
content = content.replace(
    '''boolean deleted = collection.deleteById(id);
                        if (deleted) {
                            sendJson(exchange, 204, null);''',
    '''boolean deleted = collection.deleteById(id);
                        if (deleted) {
                            logCrudEvent("DELETE", name, id, getClientIp(exchange));
                            sendJson(exchange, 204, null);'''
)

# R2: Add audit logging to KeyValueHandler PUT
content = content.replace(
    '''bucket.put(key, value);
                    sendJson(exchange, 201, java.util.Map.of("key", key, "value", value, "status", "created"));''',
    '''bucket.put(key, value);
                    logCrudEvent("PUT", bucketName + "/" + key, key, getClientIp(exchange));
                    sendJson(exchange, 201, java.util.Map.of("key", key, "value", value, "status", "created"));'''
)

# R2: Add audit logging to KeyValueHandler DELETE
content = content.replace(
    '''} else if ("DELETE".equals(exchange.getRequestMethod())) {
                    bucket.delete(key);
                    sendJson(exchange, 204, null);''',
    '''} else if ("DELETE".equals(exchange.getRequestMethod())) {
                    bucket.delete(key);
                    logCrudEvent("DELETE", bucketName + "/" + key, key, getClientIp(exchange));
                    sendJson(exchange, 204, null);'''
)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("R1+R2 changes applied to JunifyDBServer.java")
