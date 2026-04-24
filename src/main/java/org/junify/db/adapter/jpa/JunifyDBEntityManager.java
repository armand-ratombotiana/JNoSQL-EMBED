package org.junify.db.adapter.jpa;

import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;

import jakarta.persistence.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JunifyDBEntityManager implements EntityManager {

    private final JunifyDB db;
    private final String persistenceUnit;
    private boolean open = true;

    public JunifyDBEntityManager(JunifyDB db, String persistenceUnit) {
        this.db = db;
        this.persistenceUnit = persistenceUnit;
    }

    public static JunifyDBEntityManager create(EntityManagerFactory factory) {
        return null;
    }

    public void persist(Object entity) {
        var repo = getRepository(entity.getClass());
        repo.save(entity);
    }

    public <T> T merge(T entity) {
        var repo = getRepository((Class<T>) entity.getClass());
        return repo.save(entity);
    }

    public void remove(Object entity) {
        var id = getId(entity);
        var collection = getCollectionName(entity.getClass());
        db.documentCollection(collection).deleteById(String.valueOf(id));
    }

    public <T> T find(Class<T> entityClass, Object primaryKey) {
        return find(entityClass, primaryKey, null, null);
    }

    public <T> T find(Class<T> entityClass, Object primaryKey, Map<String, Object> properties) {
        return find(entityClass, primaryKey, null, properties);
    }

    public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode) {
        return find(entityClass, primaryKey, lockMode, null);
    }

    public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode, Map<String, Object> properties) {
        var collection = db.documentCollection(getCollectionName(entityClass));
        var doc = collection.findById(String.valueOf(primaryKey));
        
        if (doc == null) {
            return null;
        }
        
        return toEntity(doc, entityClass);
    }

    public <T> T getReference(Class<T> entityClass, Object primaryKey) {
        return find(entityClass, primaryKey);
    }

    public void flush() {
    }

    public void setProperty(String propertyName, Object value) {
    }

    public Map<String, Object> getProperties() {
        return new HashMap<>();
    }

    public void joinTransaction() {
    }

    public void unlock() {
    }

    public void clear() {
    }

    public boolean contains(Object entity) {
        try {
            var id = getId(entity);
            if (id == null) {
                return false;
            }
            
            var collection = getCollectionName(entity.getClass());
            return db.documentCollection(collection).findById(String.valueOf(id)) != null;
        } catch (Exception e) {
            return false;
        }
    }

    public LockModeType getLockMode(Object entity) {
        return LockModeType.NONE;
    }

    public void refresh(Object entity) {
    }

    public void refresh(Object entity, Map<String, Object> properties) {
    }

    public void refresh(Object entity, LockModeType lockMode) {
    }

    public void refresh(Object entity, LockModeType lockMode, Map<String, Object> properties) {
    }

    public Query createQuery(String qlString) {
        return new JunifyDBQuery(db, qlString);
    }

    public Query createNamedQuery(String name) {
        return createQuery(name);
    }

    public Query createNativeQuery(String sqlString) {
        return new JunifyDBQuery(db, sqlString);
    }

    public Query createNativeQuery(String sqlString, Class resultClass) {
        return createNativeQuery(sqlString).addEntity(resultClass.getSimpleName(), resultClass);
    }

    public Query createNativeQuery(String sqlString, String resultSetMapping) {
        return createNativeQuery(sqlString);
    }

    public void close() {
        this.open = false;
    }

    public boolean isOpen() {
        return open;
    }

    public EntityTransaction getTransaction() {
        return null;
    }

    public EntityManagerFactory getEntityManagerFactory() {
        return null;
    }

    public FlushModeType getFlushMode() {
        return FlushModeType.AUTO;
    }

    public void setFlushMode(FlushModeType flushMode) {
    }

    public <T> java.util.List<T> copy(Object entity) {
        return null;
    }

    public <T> T unwrap(Class<T> cls) {
        if (cls.isInstance(this)) {
            return cls.cast(this);
        }
        return null;
    }

    public Object getDelegate() {
        return db;
    }

    public void remove(Object entity, jakarta.persistence.LockModeType lockMode) {
        remove(entity);
    }

    public <T> java.util.List<T> findAll(Class<T> entityClass) {
        var collection = db.documentCollection(getCollectionName(entityClass));
        var docs = collection.findAll();
        
        return docs.stream()
            .map(doc -> toEntity(doc, entityClass))
            .collect(Collectors.toList());
    }

    public <T> java.util.List<T> findByField(Class<T> entityClass, String fieldName, Object value) {
        var collection = db.documentCollection(getCollectionName(entityClass));
        
        return collection.findAll().stream()
            .filter(doc -> Objects.equals(value, doc.get(fieldName)))
            .map(doc -> toEntity(doc, entityClass))
            .collect(Collectors.toList());
    }

    private <T> JunifyDBRepository<T, ?> getRepository(Class<T> entityClass) {
        return new JunifyDBRepository<>(db, entityClass, Object.class);
    }

    private Object getId(Object entity) {
        var clazz = entity.getClass();
        
        for (var field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            
            if (field.isAnnotationPresent(jakarta.persistence.Id.class) ||
                field.isAnnotationPresent(jakarta.nosql.Id.class)) {
                try {
                    return field.get(entity);
                } catch (Exception e) {
                    return null;
                }
            }
        }
        
        return null;
    }

    private <T> String getCollectionName(Class<T> entityClass) {
        var jpaTable = entityClass.getAnnotation(jakarta.persistence.Table.class);
        if (jpaTable != null && !jpaTable.name().isEmpty()) {
            return jpaTable.name();
        }
        
        var jpaEntity = entityClass.getAnnotation(jakarta.persistence.Entity.class);
        if (jpaEntity != null && !jpaEntity.name().isEmpty()) {
            return jpaEntity.name();
        }
        
        var nosqlEntity = entityClass.getAnnotation(jakarta.nosql.Entity.class);
        if (nosqlEntity != null && !nosqlEntity.value().isEmpty()) {
            return nosqlEntity.value();
        }
        
        return entityClass.getSimpleName().toLowerCase();
    }

    private <T> T toEntity(Document doc, Class<T> entityClass) {
        try {
            var entity = entityClass.getDeclaredConstructor().newInstance();
            var id = doc.getId();
            
            for (var field : entityClass.getDeclaredFields()) {
                field.setAccessible(true);
                
                var columnName = getColumnName(field);
                
                if (doc.containsKey(columnName)) {
                    var value = doc.get(columnName);
                    if (value != null) {
                        field.set(entity, convertValue(value, field.getType()));
                    }
                }
            }
            
            if (id != null) {
                for (var field : entityClass.getDeclaredFields()) {
                    if (field.isAnnotationPresent(jakarta.persistence.Id.class) ||
                        field.isAnnotationPresent(jakarta.nosql.Id.class)) {
                        field.setAccessible(true);
                        field.set(entity, convertValue(id, field.getType()));
                        break;
                    }
                }
            }
            
            return entity;
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert document to entity", e);
        }
    }

    private String getColumnName(java.lang.reflect.Field field) {
        var jpaCol = field.getAnnotation(jakarta.persistence.Column.class);
        if (jpaCol != null && !jpaCol.name().isEmpty()) {
            return jpaCol.name();
        }
        
        var nosqlCol = field.getAnnotation(jakarta.nosql.Column.class);
        if (nosqlCol != null && !nosqlCol.value().isEmpty()) {
            return nosqlCol.value();
        }
        
        return field.getName();
    }

    private Object convertValue(Object value, Class<?> targetType) {
        if (targetType.isInstance(value)) {
            return value;
        }
        
        if (targetType == String.class) {
            return String.valueOf(value);
        }
        
        if ((targetType == int.class || targetType == Integer.class) && value instanceof Number) {
            return ((Number) value).intValue();
        }
        
        if ((targetType == long.class || targetType == Long.class) && value instanceof Number) {
            return ((Number) value).longValue();
        }
        
        if (targetType == boolean.class || targetType == Boolean.class) {
            return Boolean.parseBoolean(String.valueOf(value));
        }
        
        return value;
    }

    private static class JunifyDBQuery implements Query {
        private final JunifyDB db;
        private final String query;
        private int maxResults = -1;
        private int firstResult = 0;

        JunifyDBQuery(JunifyDB db, String query) {
            this.db = db;
            this.query = query;
        }

        public Query setParameter(String name, Object value) {
            return this;
        }

        public Query setParameter(String name, Object value, jakarta.persistence.TemporalType temporalType) {
            return this;
        }

        public Query setParameter(String name, java.util.Calendar value, jakarta.persistence.TemporalType temporalType) {
            return this;
        }

        public Query setParameter(String name, java.util.Date value, jakarta.persistence.TemporalType temporalType) {
            return this;
        }

        public Query setParameter(int position, Object value) {
            return this;
        }

        public Query setParameter(int position, Object value, jakarta.persistence.TemporalType temporalType) {
            return this;
        }

        public Query setMaxResults(int maxResult) {
            this.maxResults = maxResult;
            return this;
        }

        public int getMaxResults() {
            return maxResults;
        }

        public Query setFirstResult(int startPosition) {
            this.firstResult = startPosition;
            return this;
        }

        public int getFirstResult() {
            return firstResult;
        }

        public Query setHint(String hintName, Object value) {
            return this;
        }

        public Query setFlushMode(jakarta.persistence.FlushModeType flushMode) {
            return this;
        }

        public Query setLockMode(jakarta.persistence.LockModeType lockMode) {
            return this;
        }

        public java.util.List getResultList() {
            return new ArrayList();
        }

        public Object getSingleResult() {
            return null;
        }

        public int executeUpdate() {
            return 0;
        }

        public Query addEntity(String entityName, Class entityClass) {
            return this;
        }

        public Query addRoot(String entityName, String alias) {
            return this;
        }

        public Query addJoin(String joinAttributeName, String alias) {
            return this;
        }

        public Query addFetchJoin(String joinAttributeName) {
            return this;
        }

        public int getParametersSize() {
            return 0;
        }

        public String getParameter(String name) {
            return null;
        }

        public int getParameter(int position) {
            return 0;
        }

        public Object getParameterValue(String name) {
            return null;
        }

        public Object getParameterValue(int position) {
            return null;
        }

        public jakarta.persistence.Parameter<?> getParameter(String name, Class clazz) {
            return null;
        }

        public jakarta.persistence.Parameter<?> getParameter(int position, Class clazz) {
            return null;
        }

        public boolean isBound(String name) {
            return false;
        }

        public boolean isBound(int position) {
            return false;
        }

        public <T> T getParameterValue(jakarta.persistence.Parameter<T> parameter) {
            return null;
        }

        public <T> jakarta.persistence.Parameter<T> getParameter(String name, Class<T> type) {
            return null;
        }

        public <T> jakarta.persistence.Parameter<T> getParameter(int position, Class<T> type) {
            return null;
        }

        public java.util.Set<jakarta.persistence.Parameter<?>> getParameters() {
            return new java.util.HashSet<>();
        }

        public String getQLString() {
            return query;
        }

        public Query setEntityGraph(String graphName, jakarta.persistence.EntityGraph<?> graph) {
            return this;
        }

        public Query addEntityGraph(String graphName, jakarta.persistence.EntityGraph<?> graph) {
            return this;
        }
    }
}