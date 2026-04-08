package org.jnosql.embed;

import org.jnosql.embed.column.ColumnFamily;
import org.jnosql.embed.config.JNoSQLConfig;
import org.jnosql.embed.document.DocumentCollection;
import org.jnosql.embed.event.EventBus;
import org.jnosql.embed.kv.KeyValueBucket;
import org.jnosql.embed.metrics.DatabaseMetrics;
import org.jnosql.embed.server.JNoSQLServer;
import org.jnosql.embed.storage.StorageEngine;
import org.jnosql.embed.transaction.Transaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JNoSQL implements Closeable {

    private final JNoSQLConfig config;
    private final StorageEngine engine;
    private final ConcurrentMap<String, DocumentCollection> collections;
    private final ConcurrentMap<String, KeyValueBucket> buckets;
    private final ConcurrentMap<String, ColumnFamily> columnFamilies;
    private final EventBus eventBus;
    private final DatabaseMetrics metrics;
    private volatile boolean closed;
    private JNoSQLServer server;

    private JNoSQL(JNoSQLConfig config) {
        this.config = config;
        this.engine = config.storageEngine().create(config.dataDir(), config.autoFlush(), config.flushIntervalMs());
        this.collections = new ConcurrentHashMap<>();
        this.buckets = new ConcurrentHashMap<>();
        this.columnFamilies = new ConcurrentHashMap<>();
        this.eventBus = new EventBus();
        this.metrics = new DatabaseMetrics();
        this.closed = false;
    }

    public static JNoSQLConfig.Builder embed() {
        return JNoSQLConfig.builder();
    }

    public static JNoSQL create(JNoSQLConfig config) {
        return new JNoSQL(config);
    }

    public DocumentCollection documentCollection(String name) {
        checkOpen();
        var col = collections.computeIfAbsent(name, n -> {
            eventBus.emit(EventBus.EventType.COLLECTION_CREATED, n);
            var collection = new DocumentCollection(n, engine, eventBus, metrics, null, config.dataDir());
            collection.loadIndexes();
            return collection;
        });
        metrics.updateCollectionSize(name, col.count());
        return col;
    }

    public KeyValueBucket keyValueBucket(String name) {
        checkOpen();
        return buckets.computeIfAbsent(name, n -> {
            eventBus.emit(EventBus.EventType.BUCKET_CREATED, n);
            return new KeyValueBucket(n, engine, eventBus, metrics);
        });
    }

    public ColumnFamily columnFamily(String name) {
        checkOpen();
        return columnFamilies.computeIfAbsent(name, n -> new ColumnFamily(n, engine));
    }

    public Transaction beginTransaction() {
        checkOpen();
        metrics.recordTransaction();
        return new Transaction(engine, eventBus, metrics);
    }

    public EventBus eventBus() {
        return eventBus;
    }

    public DatabaseMetrics metrics() {
        return metrics;
    }

    public JNoSQLServer startServer(int port) throws IOException {
        checkOpen();
        server = new JNoSQLServer(this);
        server.start(port);
        return server;
    }

    public JNoSQLConfig config() {
        return config;
    }

    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() {
        if (!closed) {
            if (server != null) server.stop();
            engine.flush();
            closed = true;
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("JNoSQL database is closed");
        }
    }

    public static void main(String[] args) {
        int port = 8080;
        String dataDir = "data";
        String engineType = "FILE";
        boolean autoFlush = true;
        int flushInterval = 1000;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--data-dir" -> dataDir = args[++i];
                case "--engine" -> engineType = args[++i];
                case "--sync" -> autoFlush = true;
                case "--async" -> autoFlush = false;
                case "--flush-interval" -> flushInterval = Integer.parseInt(args[++i]);
                case "--help" -> {
                    System.out.println("Usage: java -jar jnosql-embed.jar [options]");
                    System.out.println("Options:");
                    System.out.println("  --port <port>          Server port (default: 8080)");
                    System.out.println("  --data-dir <dir>       Data directory (default: data)");
                    System.out.println("  --engine <type>         Storage engine: FILE, IN_MEMORY (default: FILE)");
                    System.out.println("  --sync                 Enable synchronous flush (default)");
                    System.out.println("  --async                Enable asynchronous flush");
                    System.out.println("  --flush-interval <ms>  Flush interval in ms (default: 1000)");
                    System.out.println("  --help                 Show this help");
                    return;
                }
            }
        }

        var config = JNoSQL.embed()
                .storageEngine("FILE".equalsIgnoreCase(engineType) ? 
                        JNoSQLConfig.StorageEngineType.FILE : JNoSQLConfig.StorageEngineType.IN_MEMORY)
                .persistTo(dataDir)
                .autoFlush(autoFlush)
                .flushIntervalMs(flushInterval)
                .buildConfig();

        var db = JNoSQL.create(config);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            db.close();
        }));

        try {
            System.out.println("Starting JNoSQL-EMBED server on port " + port + "...");
            System.out.println("Data directory: " + dataDir);
            System.out.println("Storage engine: " + engineType);
            System.out.println("Flush mode: " + (autoFlush ? "sync" : "async"));
            db.startServer(port);
            System.out.println("Server started successfully!");
            System.out.println("API available at http://localhost:" + port + "/api");
            System.out.println("Health check at http://localhost:" + port + "/api/health");
            
            Thread.currentThread().join();
        } catch (IOException e) {
            System.err.println("Failed to start server: " + e.getMessage());
            db.close();
            System.exit(1);
        } catch (InterruptedException e) {
            System.out.println("Server interrupted, shutting down...");
            db.close();
        }
    }
}
