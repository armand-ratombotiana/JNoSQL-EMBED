package org.junify.db.adapter.jnosql;

import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.kv.KeyValueBucket;

import jakarta.nosql.Template;
import jakarta.nosql.communication.TypeReference;
import jakarta.nosql.document.DocumentCollectionManager;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import jakarta.nosql.keyvalue.KeyValueRepository;
import jakarta.nosql.keyvalue.KeyValueTemplate;

import java.util.*;
import java.util.function.Function;

public class JunifyDBTemplate implements Template, DocumentCollectionManager, KeyValueTemplate {

    private final JunifyDB db;

    public JunifyDBTemplate(JunifyDB db) {
        this.db = db;
    }

    public static JunifyDBTemplate of(JunifyDB db) {
        return new JunifyDBTemplate(db);
    }

    @Override
    public DocumentCollectionManager getDocumentCollectionManager() {
        return this;
    }

    @Override
    public KeyValueTemplate getKeyValueTemplate() {
        return this;
    }

    @Override
    public <T> T insert(T entity) {
        return insert(entity, DocumentCollectionManager.defaultLifeCycle());
    }

    @Override
    public <T> T insert(T entity, javax.nosql.communication.Timeout timeout) {
        var document = new DocumentEntity(getEntityName(entity.getClass()));
        var converter = new DocumentConverter();
        converter.convert(entity, document);
        
        var collection = db.documentCollection(getEntityName(entity.getClass()));
        var doc = toDocument(entity);
        collection.insert(doc);
        
        return entity;
    }

    @Override
    public <T> T update(T entity) {
        return insert(entity);
    }

    @Override
    public <T> void delete(Class<T> entityClass) {
        var collection = db.documentCollection(getEntityName(entityClass));
        for (var doc : collection.findAll()) {
            collection.deleteById(doc.getId());
        }
    }

    @Override
    public <T, ID> Optional<T> find(Class<T> entityClass, ID id) {
        var collection = db.documentCollection(getEntityName(entityClass));
        var doc = collection.findById(String.valueOf(id));
        if (doc == null) {
            return Optional.empty();
        }
        return Optional.of(toEntity(doc, entityClass));
    }

    @Override
    public <T, ID> void delete(Class<T> entityClass, ID id) {
        var collection = db.documentCollection(getEntityName(entityClass));
        collection.deleteById(String.valueOf(id));
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        // Convert query to delete operations
    }

    @Override
    public <T> List<T> select(DocumentQuery query) {
        var collection = db.documentCollection(query.getCollection());
        var results = collection.findAll();
        
        if (!query.getConditions().isEmpty()) {
            // Apply filters
        }
        
        return toList(results, query.getTypeReference() != null ? 
            query.getTypeReference() : new TypeReference<List<T>>() {});
    }

    // DocumentCollectionManager methods
    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        var collection = db.documentCollection(entity.getCollection());
        var doc = toDocument(entity);
        return collection.insert(doc).toEntity();
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        return insert(entity);
    }

    @Override
    public void delete(DocumentEntity entity) {
        var collection = db.documentCollection(entity.getCollection());
        collection.deleteById(entity.getId());
    }

    @Override
    public Optional<DocumentEntity> find(DocumentQuery query) {
        var collection = db.documentCollection(query.getCollection());
        var docs = collection.findAll();
        
        for (var doc : docs) {
            var entity = doc.toEntity();
            if (matchConditions(entity, query)) {
                return Optional.of(entity);
            }
        }
        
        return Optional.empty();
    }

    @Override
    public long count(String collection) {
        return db.documentCollection(collection).count();
    }

    // KeyValueTemplate methods  
    @Override
    public <T, K> T get(Class<T> entity, K key) {
        var bucket = db.keyValueBucket(getEntityName(entity));
        var value = bucket.get(String.valueOf(key));
        if (value == null) {
            return null;
        }
        return fromJson(value, entity);
    }

    @Override
    public <T, K> Iterable<T> getAll(Class<T> entity, Iterable<K> keys) {
        var bucket = db.keyValueBucket(getEntityName(entity));
        var results = new ArrayList<T>();
        
        for (K key : keys) {
            var value = bucket.get(String.valueOf(key));
            if (value != null) {
                results.add(fromJson(value, entity));
            }
        }
        
        return results;
    }

    @Override
    public <T, K> T put(Class<T> entity, K key, T value) {
        var bucket = db.keyValueBucket(getEntityName(entity));
        bucket.put(String.valueOf(key), toJson(value));
        return value;
    }

    @Override
    public <T, K> T putIfAbsent(Class<T> entity, K key, T value) {
        var bucket = db.keyValueBucket(getEntityName(entity));
        if (bucket.get(String.valueOf(key)) == null) {
            bucket.put(String.valueOf(key), toJson(value));
            return value;
        }
        return get(entity, key);
    }

    @Override
    public <T, K> void delete(Class<T> entity, K key) {
        var bucket = db.keyValueBucket(getEntityName(entity));
        bucket.delete(String.valueOf(key));
    }

    @Override
    public <T, K> boolean contains(Class<T> entity, K key) {
        var bucket = db.keyValueBucket(getEntityName(entity));
        return bucket.get(String.valueOf(key)) != null;
    }

    // Helper methods
    private String getEntityName(Class<?> entityClass) {
        var entity = entityClass.getAnnotation(jakarta.nosql.Entity.class);
        return entity != null ? entity.value() : entityClass.getSimpleName().toLowerCase();
    }

    private Document toDocument(Object entity) {
        var doc = new Document();
        var fields = entity.getClass().getDeclaredFields();
        
        for (var field : fields) {
            field.setAccessible(true);
            try {
                var value = field.get(entity);
                if (value != null) {
                    doc.add(field.getName(), value);
                }
            } catch (Exception e) {
                // Ignore
            }
        }
        
        return doc;
    }

    private <T> T toEntity(Document doc, Class<T> entityClass) {
        try {
            var entity = entityClass.getDeclaredConstructor().newInstance();
            var fields = entityClass.getDeclaredFields();
            
            for (var field : fields) {
                if (doc.containsKey(field.getName())) {
                    field.setAccessible(true);
                    field.set(entity, doc.get(field.getName()));
                }
            }
            
            return entity;
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert document to entity", e);
        }
    }

    private String toJson(Object value) {
        return org.junify.db.core.util.JsonSerde.toJson(value);
    }

    private <T> T fromJson(String json, Class<T> entityClass) {
        return org.junify.db.core.util.JsonSerde.fromJson(json, entityClass);
    }

    private boolean matchConditions(DocumentEntity entity, DocumentQuery query) {
        return true;
    }

    private <T> List<T> toList(Iterable<Document> docs, TypeReference reference) {
        var results = new ArrayList<T>();
        for (var doc : docs) {
            results.add((T) doc.toEntity());
        }
        return results;
    }
}