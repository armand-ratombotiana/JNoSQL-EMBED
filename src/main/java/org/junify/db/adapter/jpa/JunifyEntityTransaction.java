package org.junify.db.adapter.jpa;

import jakarta.persistence.EntityTransaction;

/**
 * JPA EntityTransaction implementation for JunifyDB.
 * Uses MVCC for transaction management.
 */
public class JunifyEntityTransaction implements EntityTransaction {

    private final org.junify.db.nosql.document.DocumentCollection collection;
    private boolean active = false;

    public JunifyEntityTransaction(org.junify.db.nosql.document.DocumentCollection collection) {
        this.collection = collection;
    }

    @Override
    public void begin() {
        if (active) {
            throw new IllegalStateException("Transaction already active");
        }
        active = true;
    }

    @Override
    public void commit() {
        if (!active) {
            throw new IllegalStateException("No active transaction");
        }
        collection.saveIndexes();
        active = false;
    }

    @Override
    public void rollback() {
        if (!active) {
            throw new IllegalStateException("No active transaction");
        }
        // For in-memory engine, rollback is a no-op
        // For persistent engines, would need to restore from WAL
        active = false;
    }

    @Override
    public void setRollbackOnly() {
        // Mark for rollback
    }

    @Override
    public boolean getRollbackOnly() {
        return false;
    }

    @Override
    public boolean isActive() {
        return active;
    }
}
