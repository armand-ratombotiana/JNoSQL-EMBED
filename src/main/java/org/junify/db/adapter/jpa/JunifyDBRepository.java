package org.junify.db.adapter.jpa;

import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JunifyDBRepository<T, ID> {

    private final Class<T> entityClass;
    private final Class<ID> idClass;
    private final DocumentCollection collection;
    private final JunifyDB db;

    public JunifyDBRepository(JunifyDB db, Class<T> entityClass, Class<ID> idClass) {
        this.db = db;
        this.entityClass = entityClass;
        this.idClass = idClass;
        this.collection = db.documentCollection(getEntityTableName());
    }

    public <S extends T> S save(S entity) {
        var id = getIdValue(entity);
        if (id == null) {
            id = generateId(entity);
            setIdValue(entity, id);
        }
        
        var doc = toDocument(entity);
        doc.setId(String.valueOf(id));
        collection.insert(doc);
        return entity;
    }

    public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {
        var results = new ArrayList<S>();
        for (S entity : entities) {
            results.add(save(entity));
        }
        return results;
    }

    public Optional<T> findById(ID id) {
        var doc = collection.findById(String.valueOf(id));
        if (doc == null) {
            return Optional.empty();
        }
        return Optional.of(toEntity(doc));
    }

    public Optional<T> findById(ID id, jakarta.persistence.LockModeType lockMode) {
        return findById(id);
    }

    public boolean existsById(ID id) {
        return collection.findById(String.valueOf(id)) != null;
    }

    public List<T> findAll() {
        return streamAll().collect(Collectors.toList());
    }

    public Stream<T> streamAll() {
        var docs = collection.findAll();
        return docs.stream().map(this::toEntity);
    }

    public long count() {
        return collection.count();
    }

    public void deleteById(ID id) {
        collection.deleteById(String.valueOf(id));
    }

    public void delete(T entity) {
        var id = getIdValue(entity);
        if (id != null) {
            collection.deleteById(String.valueOf(id));
        }
    }

    public void deleteAll() {
        for (var doc : collection.findAll()) {
            collection.deleteById(doc.getId());
        }
    }

    public <S extends T> List<S> findByField(String fieldName, Object value) {
        var results = new ArrayList<S>();
        for (var doc : collection.findAll()) {
            if (Objects.equals(value, doc.get(fieldName))) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public <S extends T> List<S> findByFieldContaining(String fieldName, String value) {
        var results = new ArrayList<S>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue != null && fieldValue.toString().contains(value)) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public <S extends T> List<S> findByFieldStartingWith(String fieldName, String prefix) {
        var results = new ArrayList<S>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue != null && fieldValue.toString().startsWith(prefix)) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public <S extends T> List<S> findByFieldEndingWith(String fieldName, String suffix) {
        var results = new ArrayList<S>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue != null && fieldValue.toString().endsWith(suffix)) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public <S extends T, C extends Comparable<C>> List<S> findByFieldGreaterThan(String fieldName, C value) {
        var results = new ArrayList<S>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue instanceof Comparable) {
                try {
                    @SuppressWarnings("unchecked")
                    var cmp = (Comparable<C>) fieldValue;
                    if (cmp.compareTo(value) > 0) {
                        results.add(toEntity(doc));
                    }
                } catch (Exception ignored) {}
            }
        }
        return results;
    }

    public <S extends T, C extends Comparable<C>> List<S> findByFieldLessThan(String fieldName, C value) {
        var results = new ArrayList<S>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue instanceof Comparable) {
                try {
                    @SuppressWarnings("unchecked")
                    var cmp = (Comparable<C>) fieldValue;
                    if (cmp.compareTo(value) < 0) {
                        results.add(toEntity(doc));
                    }
                } catch (Exception ignored) {}
            }
        }
        return results;
    }

    public <S extends T, C extends Comparable<C>> List<S> findByFieldBetween(String fieldName, C start, C end) {
        var results = new ArrayList<S>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (fieldValue instanceof Comparable) {
                try {
                    @SuppressWarnings("unchecked")
                    var cmp = (Comparable<C>) fieldValue;
                    if (cmp.compareTo(start) >= 0 && cmp.compareTo(end) <= 0) {
                        results.add(toEntity(doc));
                    }
                } catch (Exception ignored) {}
            }
        }
        return results;
    }

    public <S extends T> List<S> findByFieldIn(String fieldName, Collection<?> values) {
        var results = new ArrayList<S>();
        for (var doc : collection.findAll()) {
            var fieldValue = doc.get(fieldName);
            if (values.contains(fieldValue)) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public <S extends T> List<S> findByFieldNot(String fieldName, Object value) {
        var results = new ArrayList<S>();
        for (var doc : collection.findAll()) {
            if (!Objects.equals(value, doc.get(fieldName))) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public <S extends T> List<S> findByFieldIsNull(String fieldName) {
        var results = new ArrayList<S>();
        for (var doc : collection.findAll()) {
            if (doc.get(fieldName) == null) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public <S extends T> List<S> findByFieldIsNotNull(String fieldName) {
        var results = new ArrayList<S>();
        for (var doc : collection.findAll()) {
            if (doc.get(fieldName) != null) {
                results.add(toEntity(doc));
            }
        }
        return results;
    }

    public long countByField(String fieldName, Object value) {
        return findByField(fieldName, value).size();
    }

    public boolean existsByField(String fieldName, Object value) {
        return !findByField(fieldName, value).isEmpty();
    }

    public void deleteByField(String fieldName, Object value) {
        var toDelete = findByField(fieldName, value);
        for (var entity : toDelete) {
            delete(entity);
        }
    }

    public long deleteByField returningCount(String fieldName, Object value) {
        var before = count();
        deleteByField(fieldName, value);
        return before - count();
    }

    public <S extends T> S findOneByField(String fieldName, Object value) {
        return findByField(fieldName, value).stream().findFirst().orElse(null);
    }

    public int executeUpdate(String query) {
        return 0;
    }

    public <S extends T, R> List<R> findByFieldProjected(String fieldName, Object value, Class<R> projectionClass) {
        return findByField(fieldName, value).stream()
            .map(e -> project(e, projectionClass))
            .collect(Collectors.toList());
    }

    private <S extends T, R> R project(S entity, Class<R> projectionClass) {
        try {
            return projectionClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to project entity", e);
        }
    }

    private String getEntityTableName() {
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

    private ID getIdValue(T entity) {
        for (Field field : entityClass.getDeclaredFields()) {
            field.setAccessible(true);
            
            if (field.isAnnotationPresent(jakarta.persistence.Id.class) ||
                field.isAnnotationPresent(jakarta.nosql.Id.class)) {
                try {
                    @SuppressWarnings("unchecked")
                    ID id = (ID) field.get(entity);
                    return id;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to get ID value", e);
                }
            }
        }
        return null;
    }

    private void setIdValue(T entity, ID id) {
        for (Field field : entityClass.getDeclaredFields()) {
            field.setAccessible(true);
            
            if (field.isAnnotationPresent(jakarta.persistence.Id.class) ||
                field.isAnnotationPresent(jakarta.nosql.Id.class)) {
                try {
                    field.set(entity, id);
                    return;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to set ID value", e);
                }
            }
        }
    }

    private ID generateId(T entity) {
        var generatedValue = findGeneratedValueAnnotation();
        
        if (generatedValue != null) {
            var strategy = generatedValue.strategy();
            
            switch (strategy) {
                case IDENTITY:
                    return generateSequenceId();
                case SEQUENCE:
                    return generateSequenceId();
                case TABLE:
                    return generateSequenceId();
                default:
                    return generateSequenceId();
            }
        }
        
        return generateSequenceId();
    }

    private jakarta.persistence.GeneratedValue findGeneratedValueAnnotation() {
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(jakarta.persistence.GeneratedValue.class)) {
                return field.getAnnotation(jakarta.persistence.GeneratedValue.class);
            }
        }
        return null;
    }

    private ID generateSequenceId() {
        try {
            return idClass.getDeclaredConstructor(long.class).newInstance(System.nanoTime());
        } catch (Exception e) {
            return idClass.cast(UUID.randomUUID().toString());
        }
    }

    private Document toDocument(T entity) {
        var doc = new Document();
        
        for (Field field : entityClass.getDeclaredFields()) {
            field.setAccessible(true);
            
            var columnName = getColumnName(field);
            
            try {
                var value = field.get(entity);
                doc.add(columnName, value);
            } catch (Exception ignored) {}
        }
        
        return doc;
    }

    private T toEntity(Document doc) {
        try {
            var entity = entityClass.getDeclaredConstructor().newInstance();
            var id = doc.getId();
            
            for (Field field : entityClass.getDeclaredFields()) {
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
                for (Field field : entityClass.getDeclaredFields()) {
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

    private String getColumnName(Field field) {
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
        
        if ((targetType == double.class || targetType == Double.class) && value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        
        if ((targetType == float.class || targetType == Float.class) && value instanceof Number) {
            return ((Number) value).floatValue();
        }
        
        if (targetType == boolean.class || targetType == Boolean.class) {
            return Boolean.parseBoolean(String.valueOf(value));
        }
        
        if (targetType == java.util.Date.class) {
            if (value instanceof java.util.Date) {
                return value;
            }
            return new java.util.Date(Long.parseLong(String.valueOf(value)));
        }
        
        return value;
    }

    public interface Stream<T> extends Iterable<T> {
        Stream<T> filter(java.util.function.Predicate<T> predicate);
        <R> Stream<R> map(java.util.function.Function<T, R> mapper);
        void forEach(java.util.function.Consumer<T> action);
    }
}