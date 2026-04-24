package org.junify.db.adapter.jpa;

import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;

public class JunifyDBRepository<T, ID> {

    private final Class<T> entityClass;
    private final Class<ID> idClass;
    private final DocumentCollection collection;
    private final JunifyDB db;

    public JunifyDBRepository(JunifyDB db, Class<T> entityClass, Class<ID> idClass) {
        this.db = db;
        this.entityClass = entityClass;
        this.idClass = idClass;
        this.collection = db.documentCollection(getEntityName());
    }

    public <S extends T> S save(S entity) {
        var doc = toDocument(entity);
        collection.insert(doc);
        return entity;
    }

    public Optional<T> findById(ID id) {
        var doc = collection.findById(String.valueOf(id));
        if (doc == null) {
            return Optional.empty();
        }
        return Optional.of(toEntity(doc));
    }

    public List<T> findAll() {
        var docs = collection.findAll();
        var results = new ArrayList<T>();
        for (var doc : docs) {
            results.add(toEntity(doc));
        }
        return results;
    }

    public List<T> findBy(String field, Object value) {
        var results = new ArrayList<T>();
        for (var doc : collection.findAll()) {
            if (value.equals(doc.get(field))) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public long count() {
        return collection.count();
    }

    public void deleteById(ID id) {
        collection.deleteById(String.valueOf(id));
    }

    public void delete(T entity) {
        var id = getIdFieldValue(entity);
        collection.deleteById(String.valueOf(id));
    }

    public void deleteAll() {
        for (var doc : collection.findAll()) {
            collection.deleteById(doc.getId());
        }
    }

    public boolean existsById(ID id) {
        return collection.findById(String.valueOf(id)) != null;
    }

    public List<T> findAllById(Iterable<ID> ids) {
        var results = new ArrayList<T>();
        for (ID id : ids) {
            findById(id).ifPresent(results::add);
        }
        return results;
    }

    public <S extends T> List<S> saveAll(Iterable<S> entities) {
        var results = new ArrayList<S>();
        for (S entity : entities) {
            results.add(save(entity));
        }
        return results;
    }

    public void deleteAllById(Iterable<ID> ids) {
        for (ID id : ids) {
            collection.deleteById(String.valueOf(id));
        }
    }

    public List<T> findByField(String fieldName, Object value) {
        return findBy(fieldName, value);
    }

    public List<T> findByFieldContaining(String fieldName, String value) {
        var results = new ArrayList<T>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue != null && fieldValue.toString().contains(value)) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public List<T> findByFieldStartingWith(String fieldName, String prefix) {
        var results = new ArrayList<T>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue != null && fieldValue.toString().startsWith(prefix)) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public List<T> findByFieldEndingWith(String fieldName, String suffix) {
        var results = new ArrayList<T>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue != null && fieldValue.toString().endsWith(suffix)) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public List<T> findByFieldGreaterThan(String fieldName, Comparable<?> value) {
        var results = new ArrayList<T>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue != null && fieldValue instanceof Comparable) {
                if (((Comparable<?>) fieldValue).compareTo(value) > 0) {
                    results.add(toEntity(doc));
                }
            }
        }
        return results;
    }

    public List<T> findByFieldLessThan(String fieldName, Comparable<?> value) {
        var results = new ArrayList<T>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue != null && fieldValue instanceof Comparable) {
                if (((Comparable<?>) fieldValue).compareTo(value) < 0) {
                    results.add(toEntity(doc));
                }
            }
        }
        return results;
    }

    public List<T> findByFieldBetween(String fieldName, Comparable<?> start, Comparable<?> end) {
        var results = new ArrayList<T>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue != null && fieldValue instanceof Comparable) {
                var cmp = (Comparable<?>) fieldValue;
                if (cmp.compareTo(start) >= 0 && cmp.compareTo(end) <= 0) {
                    results.add(toEntity(doc));
                }
            }
        }
        return results;
    }

    public List<T> findByFieldIn(String fieldName, Collection<?> values) {
        var results = new ArrayList<T>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (values.contains(fieldValue)) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    private String getEntityName() {
        var annotation = entityClass.getAnnotation(jakarta.persistence.Entity.class);
        if (annotation != null && !annotation.name().isEmpty()) {
            return annotation.name();
        }
        return entityClass.getSimpleName().toLowerCase();
    }

    private Object getIdFieldValue(T entity) {
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(jakarta.persistence.Id.class)) {
                try {
                    field.setAccessible(true);
                    return field.get(entity);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to get ID field", e);
                }
            }
        }
        throw new IllegalStateException("No @Id field found in entity: " + entityClass.getName());
    }

    private Document toDocument(T entity) {
        var doc = new Document();
        
        for (Field field : entityClass.getDeclaredFields()) {
            field.setAccessible(true);
            try {
                var value = field.get(entity);
                doc.add(field.getName(), value);
            } catch (Exception e) {
                // Skip
            }
        }
        
        return doc;
    }

    private T toEntity(Document doc) {
        try {
            var entity = entityClass.getDeclaredConstructor().newInstance();
            
            for (Field field : entityClass.getDeclaredFields()) {
                if (doc.containsKey(field.getName())) {
                    field.setAccessible(true);
                    var value = doc.get(field.getName());
                    if (value != null) {
                        field.set(entity, convertValue(value, field.getType()));
                    }
                }
            }
            
            return entity;
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert document to entity", e);
        }
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
        
        return value;
    }
}
