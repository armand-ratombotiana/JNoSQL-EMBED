package org.junify.db.adapter.jnosql;

import jakarta.nosql.*;
import jakarta.nosql.bean.*;
import jakarta.nosql.mapping.*;
import jakarta.nosql.mapping_convert.*;

import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;

import java.util.*;
import java.util.function.Predicate;

public class JunifyDBTemplate implements Template, DocumentCollectionManager, KeyValueTemplate {

    private final JunifyDB db;

    public JunifyDBTemplate(JunifyDB db) {
        this.db = db;
    }

    public static JunifyDBTemplate of(JunifyDB db) {
        return new JunifyDBTemplate(db);
    }

    public DocumentCollectionManager getDocumentCollectionManager() {
        return this;
    }

    public KeyValueTemplate getKeyValueTemplate() {
        return this;
    }

    // Template methods
    public <T> T insert(T entity) {
        return insert(entity, null);
    }

    public <T> T insert(T entity, jakarta.nosql.communication.Timeout timeout) {
        var collection = getCollectionName(entity.getClass());
        var docCollection = db.documentCollection(collection);
        
        var document = toDocument(entity);
        docCollection.insert(document);
        
        return entity;
    }

    public <T> T update(T entity) {
        return insert(entity);
    }

    public <T> void delete(Class<T> entityClass) {
        var collection = db.documentCollection(getCollectionName(entityClass));
        for (var doc : collection.findAll()) {
            collection.deleteById(doc.getId());
        }
    }

    public <T, ID> Optional<T> find(Class<T> entityClass, ID id) {
        var collection = db.documentCollection(getCollectionName(entityClass));
        var doc = collection.findById(String.valueOf(id));
        
        if (doc == null) {
            return Optional.empty();
        }
        
        return Optional.of(toEntity(doc, entityClass));
    }

    public <T, ID> void delete(Class<T> entityClass, ID id) {
        var collection = db.documentCollection(getCollectionName(entityClass));
        collection.deleteById(String.valueOf(id));
    }

    // DocumentCollectionManager methods
    public DocumentEntity insert(DocumentEntity entity) {
        var collection = db.documentCollection(entity.getCollection());
        collection.insert(fromEntity(entity));
        return entity;
    }

    public DocumentEntity update(DocumentEntity entity) {
        return insert(entity);
    }

    public void delete(DocumentEntity entity) {
        var collection = db.documentCollection(entity.getCollection());
        collection.deleteById(entity.getId());
    }

    public Optional<DocumentEntity> find(DocumentQuery query) {
        var collection = db.documentCollection(query.getCollection());
        
        for (var doc : collection.findAll()) {
            if (matchesQuery(doc, query)) {
                return Optional.of(doc.toEntity());
            }
        }
        
        return Optional.empty();
    }

    public long count(String collection) {
        return db.documentCollection(collection).count();
    }

    // KeyValueTemplate methods
    public <T, K> T get(Class<T> entity, K key) {
        var bucket = db.keyValueBucket(getCollectionName(entity));
        var json = bucket.get(String.valueOf(key));
        
        if (json == null) {
            return null;
        }
        
        return fromJson(json, entity);
    }

    public <T, K> Iterable<T> getAll(Class<T> entity, Iterable<K> keys) {
        var bucket = db.keyValueBucket(getCollectionName(entity));
        var results = new ArrayList<T>();
        
        for (K key : keys) {
            var json = bucket.get(String.valueOf(key));
            if (json != null) {
                results.add(fromJson(json, entity));
            }
        }
        
        return results;
    }

    public <T, K> T put(Class<T> entity, K key, T value) {
        var bucket = db.keyValueBucket(getCollectionName(entity));
        bucket.put(String.valueOf(key), toJson(value));
        return value;
    }

    public <T, K> T putIfAbsent(Class<T> entity, K key, T value) {
        var bucket = db.keyValueBucket(getCollectionName(entity));
        
        if (bucket.get(String.valueOf(key)) == null) {
            bucket.put(String.valueOf(key), toJson(value));
            return value;
        }
        
        return get(entity, key);
    }

    public <T, K> void delete(Class<T> entity, K key) {
        var bucket = db.keyValueBucket(getCollectionName(entity));
        bucket.delete(String.valueOf(key));
    }

    public <T, K> boolean contains(Class<T> entity, K key) {
        var bucket = db.keyValueBucket(getCollectionName(entity));
        return bucket.get(String.valueOf(key)) != null;
    }

    // Helper methods
    private String getCollectionName(Class<?> entityClass) {
        var entityAnn = entityClass.getAnnotation(jakarta.nosql.Entity.class);
        if (entityAnn != null && !entityAnn.value().isEmpty()) {
            return entityAnn.value();
        }
        
        var jpaEntity = entityClass.getAnnotation(jakarta.persistence.Entity.class);
        if (jpaEntity != null && !jpaEntity.name().isEmpty()) {
            return jpaEntity.name();
        }
        
        return entityClass.getSimpleName().toLowerCase();
    }

    private Document toDocument(Object entity) {
        var doc = new Document();
        var clazz = entity.getClass();
        
        for (var field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            
            var idAnn = field.getAnnotation(jakarta.nosql.Id.class);
            if (idAnn != null) {
                try {
                    doc.setId(field.get(entity).toString());
                    continue;
                } catch (Exception ignored) {}
            }
            
            var jpaId = field.getAnnotation(jakarta.persistence.Id.class);
            if (jpaId != null) {
                try {
                    doc.setId(field.get(entity).toString());
                    continue;
                } catch (Exception ignored) {}
            }
            
            try {
                var value = field.get(entity);
                if (value != null) {
                    var fieldName = getFieldName(field);
                    doc.add(fieldName, value);
                }
            } catch (Exception ignored) {}
        }
        
        return doc;
    }

    private <T> T toEntity(Document doc, Class<T> entityClass) {
        try {
            var entity = entityClass.getDeclaredConstructor().newInstance();
            var idValue = doc.getId();
            
            for (var field : entityClass.getDeclaredFields()) {
                field.setAccessible(true);
                
                var fieldName = getFieldName(field);
                if (doc.containsKey(fieldName)) {
                    var value = doc.get(fieldName);
                    field.set(entity, convertValue(value, field.getType()));
                }
            }
            
            if (idValue != null) {
                for (var field : entityClass.getDeclaredFields()) {
                    if (field.isAnnotationPresent(jakarta.nosql.Id.class) ||
                        field.isAnnotationPresent(jakarta.persistence.Id.class)) {
                        field.setAccessible(true);
                        field.set(entity, idValue);
                        break;
                    }
                }
            }
            
            return entity;
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert document to entity", e);
        }
    }

    private String getFieldName(java.lang.reflect.Field field) {
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
        
        if (targetType == int.class || targetType == Integer.class) {
            return Integer.parseInt(value.toString());
        }
        
        if (targetType == long.class || targetType == Long.class) {
            return Long.parseLong(value.toString());
        }
        
        if (targetType == boolean.class || targetType == Boolean.class) {
            return Boolean.parseBoolean(value.toString());
        }
        
        if (targetType == double.class || targetType == Double.class) {
            return Double.parseDouble(value.toString());
        }
        
        if (targetType == float.class || targetType == Float.class) {
            return Float.parseFloat(value.toString());
        }
        
        return value;
    }

    private boolean matchesQuery(Document doc, DocumentQuery query) {
        if (query.getConditions().isEmpty()) {
            return true;
        }
        
        return true;
    }

    private Document fromEntity(DocumentEntity entity) {
        var doc = new Document();
        doc.setId(entity.getId());
        
        for (var entry : entity.getDocuments().entrySet()) {
            doc.add(entry.getKey(), entry.getValue());
        }
        
        return doc;
    }

    private String toJson(Object value) {
        return org.junify.db.core.util.JsonSerde.toJson(value);
    }

    private <T> T fromJson(String json, Class<T> entityClass) {
        return org.junify.db.core.util.JsonSerde.fromJson(json, entityClass);
    }
}