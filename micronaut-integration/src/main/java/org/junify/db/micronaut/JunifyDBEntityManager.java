package org.junify.db.micronaut;

import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;
import jakarta.persistence.*;
import jakarta.transaction.Transactional;

import java.lang.reflect.Field;
import java.util.*;

@Singleton
public class JunifyDBEntityManager implements EntityManager {

    private final JunifyDB db;
    private final String persistenceUnit;
    private boolean open = true;

    public JunifyDBEntityManager(JunifyDB db, String persistenceUnit) {
        this.db = db;
        this.persistenceUnit = persistenceUnit;
    }

    @Override
    public void persist(Object entity) {
        var collection = getCollectionName(entity.getClass());
        var docCollection = db.documentCollection(collection);
        
        var doc = toDocument(entity);
        var id = getIdValue(entity);
        if (id != null) {
            doc.setId(String.valueOf(id));
        } else {
            doc.setId(UUID.randomUUID().toString());
        }
        
        docCollection.insert(doc);
    }

    @Override
    public <T> T merge(T entity) {
        persist(entity);
        return entity;
    }

    @Override
    public void remove(Object entity) {
        var id = getIdValue(entity);
        if (id != null) {
            var collection = getCollectionName(entity.getClass());
            db.documentCollection(collection).deleteById(String.valueOf(id));
        }
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey) {
        var collection = db.documentCollection(getCollectionName(entityClass));
        var doc = collection.findById(String.valueOf(primaryKey));
        
        if (doc == null) {
            return null;
        }
        
        return toEntity(doc, entityClass);
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode) {
        return find(entityClass, primaryKey);
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, Map<String, Object> properties) {
        return find(entityClass, primaryKey);
    }

    @Override
    public <T> T getReference(Class<T> entityClass, Object primaryKey) {
        return find(entityClass, primaryKey);
    }

    @Override
    public void flush() {
    }

    @Override
    public void setProperty(String propertyName, Object value) {
    }

    @Override
    public Map<String, Object> getProperties() {
        return new HashMap<>();
    }

    @Override
    public void joinTransaction() {
    }

    @Override
    public void unlock() {
    }

    @Override
    public void clear() {
    }

    @Override
    public boolean contains(Object entity) {
        try {
            var id = getIdValue(entity);
            if (id == null) {
                return false;
            }
            
            var collection = getCollectionName(entity.getClass());
            return db.documentCollection(collection).findById(String.valueOf(id)) != null;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public LockModeType getLockMode(Object entity) {
        return LockModeType.NONE;
    }

    @Override
    public void refresh(Object entity) {
    }

    @Override
    public void refresh(Object entity, Map<String, Object> properties) {
    }

    @Override
    public void refresh(Object entity, LockModeType lockMode) {
    }

    @Override
    public void refresh(Object entity, LockModeType lockMode, Map<String, Object> properties) {
    }

    @Override
    public Query createQuery(String qlString) {
        return new JunifyDBQuery(db, qlString);
    }

    @Override
    public Query createNamedQuery(String name) {
        return createQuery(name);
    }

    @Override
    public Query createNativeQuery(String sqlString) {
        return new JunifyDBQuery(db, sqlString);
    }

    @Override
    public Query createNativeQuery(String sqlString, Class resultClass) {
        return new JunifyDBQuery(db, sqlString);
    }

    @Override
    public Query createNativeQuery(String sqlString, String resultSetMapping) {
        return new JunifyDBQuery(db, sqlString);
    }

    @Override
    public void close() {
        this.open = false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public EntityTransaction getTransaction() {
        return new JunifyDBTransaction(db);
    }

    @Override
    public EntityManagerFactory getEntityManagerFactory() {
        return null;
    }

    @Override
    public FlushModeType getFlushMode() {
        return FlushModeType.AUTO;
    }

    @Override
    public void setFlushMode(FlushModeType flushMode) {
    }

    @Override
    public <T> List<T> findAll(Class<T> entityClass) {
        var collection = db.documentCollection(getCollectionName(entityClass));
        
        return collection.findAll().stream()
            .map(doc -> toEntity(doc, entityClass))
            .collect(Collectors.toList());
    }

    @Override
    public <T> List<T> findByField(Class<T> entityClass, String fieldName, Object value) {
        var collection = db.documentCollection(getCollectionName(entityClass));
        
        return collection.findAll().stream()
            .filter(doc -> Objects.equals(value, doc.get(fieldName)))
            .map(doc -> toEntity(doc, entityClass))
            .collect(Collectors.toList());
    }

    private String getCollectionName(Class<?> entityClass) {
        var table = entityClass.getAnnotation(jakarta.persistence.Table.class);
        if (table != null && !table.name().isEmpty()) {
            return table.name();
        }
        
        var entity = entityClass.getAnnotation(jakarta.persistence.Entity.class);
        if (entity != null && !entity.name().isEmpty()) {
            return entity.name();
        }
        
        var nosqlEntity = entityClass.getAnnotation(jakarta.nosql.Entity.class);
        if (nosqlEntity != null && !nosqlEntity.value().isEmpty()) {
            return nosqlEntity.value();
        }
        
        return entityClass.getSimpleName().toLowerCase();
    }

    private Object getIdValue(Object entity) {
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

    private <T> Document toDocument(T entity) {
        var doc = new Document();
        var clazz = entity.getClass();
        
        for (var field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            
            try {
                var value = field.get(entity);
                var columnName = getColumnName(field);
                doc.add(columnName, value);
            } catch (Exception ignored) {
            }
        }
        
        return doc;
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
        var col = field.getAnnotation(jakarta.persistence.Column.class);
        if (col != null && !col.name().isEmpty()) {
            return col.name();
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

    public class JunifyDBTransaction implements EntityTransaction {
        private final JunifyDB db;
        private boolean active = false;

        public JunifyDBTransaction(JunifyDB db) {
            this.db = db;
        }

        @Override
        public void begin() {
            this.active = true;
        }

        @Override
        public void commit() {
            this.active = false;
        }

        @Override
        public void rollback() {
            this.active = false;
        }

        @Override
        public boolean isActive() {
            return active;
        }
    }

    public class JunifyDBQuery implements Query {
        private final JunifyDB db;
        private final String query;
        private int maxResults = Integer.MAX_VALUE;
        private int firstResult = 0;

        public JunifyDBQuery(JunifyDB db, String query) {
            this.db = db;
            this.query = query;
        }

        @Override
        public Query setParameter(String name, Object value) {
            return this;
        }

        @Override
        public Query setParameter(String name, Object value, jakarta.persistence.TemporalType temporalType) {
            return this;
        }

        @Override
        public Query setParameter(int position, Object value) {
            return this;
        }

        @Override
        public Query setMaxResults(int maxResult) {
            this.maxResults = maxResult;
            return this;
        }

        @Override
        public int getMaxResults() {
            return maxResults;
        }

        @Override
        public Query setFirstResult(int startPosition) {
            this.firstResult = startPosition;
            return this;
        }

        @Override
        public int getFirstResult() {
            return firstResult;
        }

        @Override
        public Query setHint(String hintName, Object value) {
            return this;
        }

        @Override
        public Query setFlushMode(jakarta.persistence.FlushModeType flushMode) {
            return this;
        }

        @Override
        public List getResultList() {
            return new ArrayList();
        }

        @Override
        public Object getSingleResult() {
            return null;
        }

        @Override
        public int executeUpdate() {
            return 0;
        }

        @Override
        public int getParametersSize() {
            return 0;
        }

        @Override
        public String getParameter(String name) {
            return null;
        }

        @Override
        public int getParameter(int position) {
            return 0;
        }

        @Override
        public Object getParameterValue(String name) {
            return null;
        }

        @Override
        public Object getParameterValue(int position) {
            return null;
        }

        @Override
        public jakarta.persistence.Parameter<?> getParameter(String name) {
            return null;
        }

        @Override
        public jakarta.persistence.Parameter<?> getParameter(int position) {
            return null;
        }

        @Override
        public boolean isBound(String name) {
            return false;
        }

        @Override
        public boolean isBound(int position) {
            return false;
        }

        @Override
        public <T> T getParameterValue(jakarta.persistence.Parameter<T> parameter) {
            return null;
        }

        @Override
        public Set<jakarta.persistence.Parameter<?>> getParameters() {
            return new HashSet<>();
        }
    }
}