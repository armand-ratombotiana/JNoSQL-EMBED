package org.jnosql.embed;

import org.jnosql.embed.column.ColumnFamily;
import org.jnosql.embed.config.JNoSQLConfig;
import org.jnosql.embed.document.DocumentCollection;
import org.jnosql.embed.kv.KeyValueBucket;
import org.jnosql.embed.storage.StorageEngine;
import org.jnosql.embed.transaction.Transaction;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JNoSQL implements Closeable {

    private final JNoSQLConfig config;
    private final StorageEngine engine;
    private final ConcurrentMap<String, DocumentCollection> collections;
    private final ConcurrentMap<String, KeyValueBucket> buckets;
    private final ConcurrentMap<String, ColumnFamily> columnFamilies;
    private volatile boolean closed;

    private JNoSQL(JNoSQLConfig config) {
        this.config = config;
        this.engine = config.storageEngine().create(config.dataDir());
        this.collections = new ConcurrentHashMap<>();
        this.buckets = new ConcurrentHashMap<>();
        this.columnFamilies = new ConcurrentHashMap<>();
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
        return collections.computeIfAbsent(name, n -> new DocumentCollection(n, engine));
    }

    public KeyValueBucket keyValueBucket(String name) {
        checkOpen();
        return buckets.computeIfAbsent(name, n -> new KeyValueBucket(n, engine));
    }

    public ColumnFamily columnFamily(String name) {
        checkOpen();
        return columnFamilies.computeIfAbsent(name, n -> new ColumnFamily(n, engine));
    }

    public Transaction beginTransaction() {
        checkOpen();
        return new Transaction(engine);
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
            engine.flush();
            closed = true;
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("JNoSQL database is closed");
        }
    }
}
