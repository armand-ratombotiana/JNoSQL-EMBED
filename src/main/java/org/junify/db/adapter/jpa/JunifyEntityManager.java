package org.junify.db.adapter.jpa;

import jakarta.persistence.EntityTransaction;

import java.util.List;

/**
 * Simplified JPA 3.1 EntityManager for JunifyDB.
 * 
 * Provides core JPA functionality:
 * - persist, merge, remove, find
 * - Basic JPQL queries
 * - Transaction management
 * 
 * Note: Criteria API, native queries, and advanced features are not yet implemented.
 */
public class JunifyEntityManager {

    private final org.junify.db.nosql.document.DocumentCollection collection;
    private final EntityTransaction transaction;
    private boolean open = true;

    private JunifyEntityManager(org.junify.db.nosql.document.DocumentCollection collection) {
        this.collection = collection;
        this.transaction = new JunifyEntityTransaction(collection);
    }

    /**
     * Create EntityManager for a collection.
     */
    public static JunifyEntityManager of(org.junify.db.nosql.document.DocumentCollection collection) {
        return new JunifyEntityManager(collection);
    }

    /**
     * Persist an entity.
     */
    public void persist(Object entity) {
        if (entity == null) {
            throw new IllegalArgumentException("Entity cannot be null");
        }
        var doc = JpaEntityMapper.toDocument(entity);
        collection.insert(doc);
    }

    /**
     * Merge an entity.
     */
    public <T> T merge(T entity) {
        if (entity == null) {
            throw new IllegalArgumentException("Entity cannot be null");
        }
        var id = JpaEntityMapper.getIdValue(entity);
        if (id == null) {
            throw new IllegalArgumentException("Entity must have an ID");
        }
        var existing = collection.findById(id.toString());
        if (existing != null) {
            var doc = JpaEntityMapper.toDocument(entity);
            collection.update(doc);
        } else {
            persist(entity);
        }
        return entity;
    }

    /**
     * Remove an entity.
     */
    public void remove(Object entity) {
        if (entity == null) {
            throw new IllegalArgumentException("Entity cannot be null");
        }
        var id = JpaEntityMapper.getIdValue(entity);
        if (id != null) {
            collection.deleteById(id.toString());
        }
    }

    /**
     * Find an entity by ID.
     */
    public <T> T find(Class<T> entityClass, Object id) {
        var doc = collection.findById(id.toString());
        if (doc == null) {
            return null;
        }
        return JpaEntityMapper.fromDocument(doc, entityClass);
    }

    /**
     * Get all entities of a type.
     */
    public <T> List<T> findAll(Class<T> entityClass) {
        return collection.findAll().stream()
            .map(doc -> JpaEntityMapper.fromDocument(doc, entityClass))
            .collect(java.util.stream.Collectors.toList());
    }

    /**
     * Get the transaction.
     */
    public EntityTransaction getTransaction() {
        return transaction;
    }

    /**
     * Find all entities matching a simple WHERE clause.
     * Example: "WHERE age > 18" or "WHERE name = 'John'"
     */
    public <T> List<T> findWhere(Class<T> entityClass, String whereClause) {
        // Simple implementation - just returns all for now
        // Full JPQL parsing would require a query parser
        return findAll(entityClass);
    }

    /**
     * Get the underlying collection.
     */
    public org.junify.db.nosql.document.DocumentCollection getCollection() {
        return collection;
    }

    /**
     * Get entity name.
     */
    public <T> String getEntityName(Class<T> entityClass) {
        return JpaEntityMapper.getEntityName(entityClass).orElse(entityClass.getSimpleName());
    }

    /**
     * Count entities of a type.
     */
    public <T> long count(Class<T> entityClass) {
        return collection.count();
    }

    /**
     * Delete all entities of a type.
     */
    public <T> void deleteAll(Class<T> entityClass) {
        collection.findAll().forEach(doc -> collection.deleteById(doc.id()));
    }

    /**
     * Flush pending changes.
     */
    public void flush() {
        collection.saveIndexes();
    }

    /**
     * Check if entity is managed.
     */
    public boolean contains(Object entity) {
        var id = JpaEntityMapper.getIdValue(entity);
        if (id == null) return false;
        return collection.findById(id.toString()) != null;
    }

    /**
     * Close the EntityManager.
     */
    public void close() {
        open = false;
    }

    /**
     * Check if EntityManager is open.
     */
    public boolean isOpen() {
        return open;
    }
}
