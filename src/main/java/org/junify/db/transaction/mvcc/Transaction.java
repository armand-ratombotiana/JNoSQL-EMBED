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
    private final List<Operation> operations;
    private volatile Status status;
    private final Instant createdAt;
    private volatile long timeoutMs;
    private volatile Instant lastActivity;

    public Transaction(StorageEngine engine, EventBus eventBus, DatabaseMetrics metrics) {
        this(engine, eventBus, metrics, 30000);
    }

    public Transaction(StorageEngine engine, EventBus eventBus, DatabaseMetrics metrics, long timeoutMs) {
        this.id = UUID.randomUUID().toString();
        this.engine = engine;
        this.eventBus = eventBus;
        this.metrics = metrics;
        this.operations = new ArrayList<>();
        this.status = Status.ACTIVE;
        this.createdAt = Instant.now();
        this.timeoutMs = timeoutMs;
        this.lastActivity = Instant.now();
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
        for (var op : operations) {
            switch (op.type()) {
                case PUT -> engine.put(op.collection(), op.key(), op.value());
                case DELETE -> engine.delete(op.collection(), op.key());
            }
        }
        status = Status.COMMITTED;
        metrics.recordTransactionCommit();
        eventBus.emit(EventBus.EventType.AFTER_COMMIT, id);
    }

    public void rollback() {
        if (status == Status.COMMITTED) {
            throw new IllegalStateException("Cannot rollback a committed transaction");
        }
        if (status == Status.ROLLED_BACK) {
            return;
        }
        eventBus.emit(EventBus.EventType.BEFORE_ROLLBACK, id);
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
            "lastActivity", lastActivity.toString()
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
