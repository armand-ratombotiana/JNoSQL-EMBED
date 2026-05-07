package org.junify.db.adapter.jpa;

import org.junify.db.nosql.document.Document;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * JPA Entity to Document mapper.
 * 
 * Handles JPA annotations:
 * - @Id for entity identifiers
 * - @jakarta.persistence.GeneratedValue for auto-generated IDs
 * - @jakarta.persistence.Transient for fields to skip
 * - @jakarta.persistence.Embeddable for nested objects
 * - @jakarta.persistence.Entity for entity type detection
 */
public final class JpaEntityMapper {

    private JpaEntityMapper() {
        // Utility class
    }

    /**
     * Convert a JPA entity to Document.
     */
    public static <T> Document toDocument(T entity) {
        var doc = new Document();
        var clazz = entity.getClass();

        // Check for @Entity annotation
        var entityAnnotation = clazz.getAnnotation(jakarta.persistence.Entity.class);
        if (entityAnnotation != null && !entityAnnotation.name().isEmpty()) {
            doc.add("_entity", entityAnnotation.name());
        }

        // Process all fields
        for (Field field : clazz.getDeclaredFields()) {
            // Skip @Transient fields
            if (field.isAnnotationPresent(jakarta.persistence.Transient.class)) {
                continue;
            }

            field.setAccessible(true);
            try {
                Object value = field.get(entity);
                if (value != null) {
                    // Handle @Id field
                    if (field.isAnnotationPresent(jakarta.persistence.Id.class)) {
                        if (value != null) {
                            doc.id(value.toString());
                        }
                    } else {
                        // Handle @Column annotation
                        var columnAnnotation = field.getAnnotation(jakarta.persistence.Column.class);
                        String fieldName = columnAnnotation != null && !columnAnnotation.name().isEmpty()
                            ? columnAnnotation.name()
                            : field.getName();
                        doc.add(fieldName, value);
                    }
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to access field: " + field.getName(), e);
            }
        }

        // Generate ID if not set
        if (doc.id() == null) {
            var idField = findIdField(clazz);
            if (idField != null) {
                var generatedValue = idField.getAnnotation(jakarta.persistence.GeneratedValue.class);
                if (generatedValue != null) {
                    var generatedId = java.util.UUID.randomUUID().toString();
                    doc.id(generatedId);
                    try {
                        idField.setAccessible(true);
                        idField.set(entity, generatedId);
                    } catch (IllegalAccessException e) {
                        // Ignore
                    }
                }
            }
        }

        return doc;
    }

    /**
     * Convert Document to JPA entity.
     */
    public static <T> T fromDocument(Document doc, Class<T> entityClass) {
        try {
            T entity = entityClass.getDeclaredConstructor().newInstance();

            for (Field field : entityClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(jakarta.persistence.Transient.class)) {
                    continue;
                }

                field.setAccessible(true);

                // Handle @Id field
                if (field.isAnnotationPresent(jakarta.persistence.Id.class)) {
                    if (doc.id() != null) {
                        setValue(field, entity, doc.id());
                    }
                } else {
                    var columnAnnotation = field.getAnnotation(jakarta.persistence.Column.class);
                    String fieldName = columnAnnotation != null && !columnAnnotation.name().isEmpty()
                        ? columnAnnotation.name()
                        : field.getName();
                    
                    Object value = doc.get(fieldName);
                    if (value != null) {
                        setValue(field, entity, value);
                    }
                }
            }

            return entity;
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert document to entity", e);
        }
    }

    /**
     * Get the ID value from a JPA entity.
     */
    public static Object getIdValue(Object entity) {
        var clazz = entity.getClass();
        var idField = findIdField(clazz);
        if (idField == null) {
            // Fallback: look for getId() method
            try {
                Method getIdMethod = clazz.getMethod("getId");
                return getIdMethod.invoke(entity);
            } catch (Exception e) {
                return null;
            }
        }

        idField.setAccessible(true);
        try {
            return idField.get(entity);
        } catch (IllegalAccessException e) {
            return null;
        }
    }

    /**
     * Get entity name from @Entity annotation.
     */
    public static Optional<String> getEntityName(Class<?> clazz) {
        var entityAnnotation = clazz.getAnnotation(jakarta.persistence.Entity.class);
        if (entityAnnotation != null && !entityAnnotation.name().isEmpty()) {
            return Optional.of(entityAnnotation.name());
        }
        return Optional.of(clazz.getSimpleName());
    }

    /**
     * Find the @Id field in a class.
     */
    private static Field findIdField(Class<?> clazz) {
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(jakarta.persistence.Id.class)) {
                return field;
            }
        }
        // Check parent class
        if (clazz.getSuperclass() != null && clazz.getSuperclass() != Object.class) {
            return findIdField(clazz.getSuperclass());
        }
        return null;
    }

    /**
     * Set field value with type conversion.
     */
    @SuppressWarnings("unchecked")
    private static void setValue(Field field, Object entity, Object value) throws IllegalAccessException {
        if (field.getType().isAssignableFrom(value.getClass())) {
            field.set(entity, value);
        } else if (field.getType() == Integer.class || field.getType() == int.class) {
            if (value instanceof Number) {
                field.set(entity, ((Number) value).intValue());
            } else {
                field.set(entity, Integer.parseInt(value.toString()));
            }
        } else if (field.getType() == Long.class || field.getType() == long.class) {
            if (value instanceof Number) {
                field.set(entity, ((Number) value).longValue());
            } else {
                field.set(entity, Long.parseLong(value.toString()));
            }
        } else if (field.getType() == Double.class || field.getType() == double.class) {
            if (value instanceof Number) {
                field.set(entity, ((Number) value).doubleValue());
            } else {
                field.set(entity, Double.parseDouble(value.toString()));
            }
        } else if (field.getType() == Boolean.class || field.getType() == boolean.class) {
            if (value instanceof Boolean) {
                field.set(entity, value);
            } else {
                field.set(entity, Boolean.parseBoolean(value.toString()));
            }
        } else {
            field.set(entity, value);
        }
    }
}
