package org.junify.db.transaction.mvcc;

import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.core.event.EventBus;
import org.junify.db.core.metrics.DatabaseMetrics;
import org.junify.db.storage.spi.StorageEngine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Transaction implements AutoCloseable {

    public enum Status { ACTIVE, COMMITTED, ROLLED_BACK, TIMEOUT }

    private final String id;
    private final StorageEngine engine;
    private final EventBus eventBus;
    private final DatabaseMetrics metrics;
    private final MVCCManager mvcc;
    private final List<Operation> operations;
    private final long readTimestamp;
    private volatile Status status;
    private final Instant createdAt;
    private volatile long timeoutMs;
    private volatile Instant lastActivity;

    public Transaction(StorageEngine engine, EventBus eventBus, DatabaseMetrics metrics) {
        this(engine, eventBus, metrics, 30000, null);
    }

    public Transaction(StorageEngine engine, EventBus eventBus, DatabaseMetrics metrics, long timeoutMs) {
        this(engine, eventBus, metrics, timeoutMs, null);
    }

    public Transaction(StorageEngine engine, EventBus eventBus, DatabaseMetrics metrics, MVCCManager mvcc) {
        this(engine, eventBus, metrics, 30000, mvcc);
    }

    public Transaction(StorageEngine engine, EventBus eventBus, DatabaseMetrics metrics, long timeoutMs, MVCCManager mvcc) {
        this.id = UUID.randomUUID().toString();
        this.engine = engine;
        this.eventBus = eventBus;
        this.metrics = metrics;
        this.mvcc = mvcc;
        this.operations = new ArrayList<>();
        this.status = Status.ACTIVE;
        this.createdAt = Instant.now();
        this.readTimestamp = mvcc != null ? mvcc.assignTimestamp() : Long.MAX_VALUE;
        this.timeoutMs = timeoutMs;
        this.lastActivity = Instant.now();
    }

    /**
     * Java 25: The snapshot read timestamp — all reads in this transaction see data
     * committed before or at this timestamp.
     */
    public long readTimestamp() {
        return readTimestamp;
    }

    /**
     * Java 25: Read a value with MVCC snapshot isolation.
     * Returns the version visible at this transaction's read timestamp.
     */
    public String read(String collection, String key) {
        checkOpen();
        lastActivity = Instant.now();

        // Check local write buffer first
        for (int i = operations.size() - 1; i >= 0; i--) {
            var op = operations.get(i);
            if (op.collection().equals(collection) && op.key().equals(key)) {
                return op.type() == Operation.Type.DELETE ? null : op.value();
            }
        }

        // Read from MVCC or engine
        if (mvcc != null) {
            var record = mvcc.read(collection + ":" + key, readTimestamp, null);
            if (record != null) return record.toJson();
        }
        return engine.get(collection, key);
    }

    /**
     * Java 25: Write a value staged for this transaction.
     */
    public void write(String collection, String key, String value) {
        checkOpen();
        lastActivity = Instant.now();
        if (mvcc != null) {
            var doc = Document.fromJson(value);
            mvcc.stageWrite(id, collection + ":" + key, doc);
        }
        operations.add(new Operation(Operation.Type.PUT, collection, key, value));
    }

    /**
     * Java 25: Delete a key staged for this transaction.
     */
    public void delete(String collection, String key) {
        checkOpen();
        lastActivity = Instant.now();
        if (mvcc != null) {
            mvcc.stageDelete(id, collection + ":" + key);
        }
        operations.add(new Operation(Operation.Type.DELETE, collection, key, null));
    }

    public String id() {
        return id;
    }

    public Status status() {
        return status;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public long timeoutMs() {
        return timeoutMs;
    }

    public Transaction timeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public int operationCount() {
        return operations.size();
    }

    public DocumentCollection documentCollection(String name) {
        checkOpen();
        lastActivity = Instant.now();
        return new TransactionalCollection(name, engine, operations, eventBus, metrics);
    }

    public void commit() {
        checkOpen();
        eventBus.emit(EventBus.EventType.BEFORE_COMMIT, id);

        boolean mvccOk = true;
        if (mvcc != null) {
            long commitTs = mvcc.assignTimestamp();
            mvccOk = mvcc.commit(id, commitTs);
        }

        if (mvccOk) {
            for (var op : operations) {
                switch (op.type()) {
                    case PUT -> engine.put(op.collection(), op.key(), op.value());
                    case DELETE -> engine.delete(op.collection(), op.key());
                }
            }
        }

        status = mvccOk ? Status.COMMITTED : Status.ROLLED_BACK;
        if (mvccOk) {
            metrics.recordTransactionCommit();
            eventBus.emit(EventBus.EventType.AFTER_COMMIT, id);
        } else {
            metrics.recordTransactionRollback();
            eventBus.emit(EventBus.EventType.AFTER_ROLLBACK, id);
            throw new IllegalStateException("Transaction " + id + " aborted due to write-write conflict");
        }
    }

    public void rollback() {
        if (status == Status.COMMITTED) {
            throw new IllegalStateException("Cannot rollback a committed transaction");
        }
        if (status == Status.ROLLED_BACK) {
            return;
        }
        eventBus.emit(EventBus.EventType.BEFORE_ROLLBACK, id);

        if (mvcc != null) {
            mvcc.rollback(id);
        }

        status = Status.ROLLED_BACK;
        operations.clear();
        metrics.recordTransactionRollback();
        eventBus.emit(EventBus.EventType.AFTER_ROLLBACK, id);
    }

    public boolean isOpen() {
        return status == Status.ACTIVE;
    }

    public boolean isCommitted() {
        return status == Status.COMMITTED;
    }

    public boolean isRolledBack() {
        return status == Status.ROLLED_BACK;
    }

    @Override
    public void close() {
        if (status == Status.ACTIVE) {
            rollback();
        }
    }

    public Map<String, Object> info() {
        return Map.of(
            "id", id,
            "status", status.name(),
            "createdAt", createdAt.toString(),
            "operations", operations.size(),
            "timeoutMs", timeoutMs,
            "lastActivity", lastActivity.toString(),
            "isolationLevel", mvcc != null ? "SNAPSHOT" : "READ_COMMITTED",
            "readTimestamp", readTimestamp
        );
    }

    private void checkOpen() {
        if (status != Status.ACTIVE) {
            throw new IllegalStateException("Transaction " + id + " is " + status.name().toLowerCase());
        }
    }

    private record Operation(Type type, String collection, String key, String value) {
        enum Type { PUT, DELETE }
    }

    private class TransactionalCollection extends DocumentCollection {

        private final List<Operation> ops;

        TransactionalCollection(String name, StorageEngine engine, List<Operation> ops, EventBus eventBus, DatabaseMetrics metrics) {
            super(name, engine, eventBus, metrics);
            this.ops = ops;
        }

        @Override
        public Document insert(Document doc) {
            if (doc.id() == null) {
                doc.id(UUID.randomUUID().toString());
            }
            ops.add(new Operation(Operation.Type.PUT, name(), doc.id(), doc.toJson()));
            return doc;
        }

        @Override
        public boolean deleteById(String id) {
            ops.add(new Operation(Operation.Type.DELETE, name(), id, null));
            return true;
        }
    }
}
