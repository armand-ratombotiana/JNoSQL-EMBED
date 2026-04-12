package org.junify.db;

import org.junify.db.nosql.column.ColumnFamily;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.core.event.EventBus;
import org.junify.db.nosql.kv.KeyValueBucket;
import org.junify.db.core.metrics.DatabaseMetrics;
import org.junify.db.console.http.JunifyDBServer;
import org.junify.db.storage.spi.H2StorageEngine;
import org.junify.db.storage.spi.StorageEngine;
import org.junify.db.transaction.mvcc.Transaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JunifyDB implements Closeable {

    private final JunifyDBConfig config;
    private final StorageEngine engine;
    private final ConcurrentMap<String, DocumentCollection> collections;
    private final ConcurrentMap<String, KeyValueBucket> buckets;
    private final ConcurrentMap<String, ColumnFamily> columnFamilies;
    private final EventBus eventBus;
    private final DatabaseMetrics metrics;
    private volatile boolean closed;
    private JunifyDBServer server;

    private JunifyDB(JunifyDBConfig config) {
        this.config = config;
        this.engine = config.storageEngine().create(config.dataDir(), config.autoFlush(), config.flushIntervalMs());
        this.collections = new ConcurrentHashMap<>();
        this.buckets = new ConcurrentHashMap<>();
        this.columnFamilies = new ConcurrentHashMap<>();
        this.eventBus = new EventBus();
        this.metrics = new DatabaseMetrics();
        this.closed = false;
    }

    public static JunifyDBConfig.Builder embed() {
        return JunifyDBConfig.builder();
    }

    public static JunifyDB create(JunifyDBConfig config) {
        return new JunifyDB(config);
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

    public H2StorageEngine h2Engine() {
        if (engine instanceof H2StorageEngine h2) {
            return h2;
        }
        throw new UnsupportedOperationException("SQL execution is only available with H2 storage engine");
    }

    public boolean isH2Engine() {
        return engine instanceof H2StorageEngine;
    }

    public JunifyDBServer startServer(int port) throws IOException {
        checkOpen();
        server = new JunifyDBServer(this);
        server.start(port);
        return server;
    }

    public JunifyDBConfig config() {
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

    public void flush() {
        engine.flush();
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("JunifyDB database is closed");
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
                    System.out.println("Usage: java -jar junify-embed.jar [options]");
                    System.out.println("Options:");
                    System.out.println("  --port <port>          Server port (default: 8080)");
                    System.out.println("  --data-dir <dir>       Data directory (default: data)");
                    System.out.println("  --engine <type>         Storage engine: FILE, IN_MEMORY, LSM_TREE, B_TREE, H2 (default: FILE)");
                    System.out.println("  --sync                 Enable synchronous flush (default)");
                    System.out.println("  --async                Enable asynchronous flush");
                    System.out.println("  --flush-interval <ms>  Flush interval in ms (default: 1000)");
                    System.out.println("  --help                 Show this help");
                    return;
                }
            }
        }

        var config = JunifyDB.embed()
                .storageEngine(switch (engineType.toUpperCase()) {
                    case "FILE" -> JunifyDBConfig.StorageEngineType.FILE;
                    case "LSM_TREE" -> JunifyDBConfig.StorageEngineType.LSM_TREE;
                    case "B_TREE" -> JunifyDBConfig.StorageEngineType.B_TREE;
                    case "H2" -> JunifyDBConfig.StorageEngineType.H2;
                    default -> JunifyDBConfig.StorageEngineType.IN_MEMORY;
                })
                .persistTo(dataDir)
                .autoFlush(autoFlush)
                .flushIntervalMs(flushInterval)
                .buildConfig();

        var db = JunifyDB.create(config);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            db.close();
        }));

        try {
            System.out.println("Starting junify-EMBED server on port " + port + "...");
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
