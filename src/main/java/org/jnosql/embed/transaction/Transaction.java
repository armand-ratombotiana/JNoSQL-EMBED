package org.jnosql.embed.transaction;

import org.jnosql.embed.document.Document;
import org.jnosql.embed.document.DocumentCollection;
import org.jnosql.embed.storage.StorageEngine;

import java.util.ArrayList;
import java.util.List;

public class Transaction implements AutoCloseable {

    private final StorageEngine engine;
    private final List<Operation> operations;
    private boolean committed;
    private boolean rolledBack;

    public Transaction(StorageEngine engine) {
        this.engine = engine;
        this.operations = new ArrayList<>();
        this.committed = false;
        this.rolledBack = false;
    }

    public DocumentCollection documentCollection(String name) {
        checkOpen();
        return new TransactionalCollection(name, engine, operations);
    }

    public void commit() {
        checkOpen();
        for (var op : operations) {
            switch (op.type()) {
                case PUT -> engine.put(op.collection(), op.key(), op.value());
                case DELETE -> engine.delete(op.collection(), op.key());
            }
        }
        committed = true;
    }

    public void rollback() {
        if (committed) {
            throw new IllegalStateException("Cannot rollback a committed transaction");
        }
        rolledBack = true;
        operations.clear();
    }

    public boolean isCommitted() {
        return committed;
    }

    public boolean isRolledBack() {
        return rolledBack;
    }

    @Override
    public void close() {
        if (!committed && !rolledBack) {
            rollback();
        }
    }

    private void checkOpen() {
        if (committed || rolledBack) {
            throw new IllegalStateException("Transaction is closed");
        }
    }

    private record Operation(Type type, String collection, String key, String value) {
        enum Type { PUT, DELETE }
    }

    private class TransactionalCollection extends DocumentCollection {

        private final List<Operation> ops;

        TransactionalCollection(String name, StorageEngine engine, List<Operation> ops) {
            super(name, engine);
            this.ops = ops;
        }

        @Override
        public Document insert(Document doc) {
            if (doc.id() == null) {
                doc.id(java.util.UUID.randomUUID().toString());
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
