package org.junify.db.adapter.jnosql;

import org.junify.db.document.Document;
import org.junify.db.core.util.JsonSerde;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

public class EntityMapper {

    public static <T> Document toDocument(T entity) {
        Class<?> clazz = entity.getClass();
        Entity annotation = clazz.getAnnotation(Entity.class);
        String collectionName = annotation != null && !annotation.value().isEmpty() 
            ? annotation.value() 
            : clazz.getSimpleName().toLowerCase();

        Document doc = new Document();
        doc.add("_entity", collectionName);
        
        for (Field field : clazz.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
                continue;
            }
            
            if (field.isAnnotationPresent(Transient.class)) {
                continue;
            }
            
            field.setAccessible(true);
            try {
                Object value = field.get(entity);
                if (value != null) {
                    if (field.isAnnotationPresent(Id.class)) {
                        doc.id(value != null ? value.toString() : null);
                    } else {
                        String fieldName;
                        if (field.isAnnotationPresent(Column.class)) {
                            String colValue = field.getAnnotation(Column.class).value();
                            fieldName = !colValue.isEmpty() ? colValue : field.getName();
                        } else {
                            fieldName = field.getName();
                        }
                        doc.add(fieldName, value);
                    }
                }
            } catch (IllegalAccessException e) {
                // Skip inaccessible fields
            }
        }
        
        return doc;
    }

    public static <T> T fromDocument(Document doc, Class<T> clazz) {
        try {
            T entity = clazz.getDeclaredConstructor().newInstance();
            
            for (Field field : clazz.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
                    continue;
                }
                
                if (field.isAnnotationPresent(Transient.class)) {
                    continue;
                }
                
                field.setAccessible(true);
                
                String fieldName;
                if (field.isAnnotationPresent(Id.class)) {
                    fieldName = "id";
                } else if (field.isAnnotationPresent(Column.class)) {
                    String colValue = field.getAnnotation(Column.class).value();
                    fieldName = !colValue.isEmpty() ? colValue : field.getName();
                } else {
                    fieldName = field.getName();
                }
                
                Object value = doc.get(fieldName);
                if (value != null) {
                    field.set(entity, convertValue(value, field.getType()));
                }
            }
            
            return entity;
        } catch (Exception e) {
            throw new RuntimeException("Failed to map document to entity: " + clazz.getName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T convertValue(Object value, Class<T> targetType) {
        if (value == null) return null;
        
        if (targetType.isInstance(value)) {
            return (T) value;
        }
        
        if (targetType == String.class) {
            return (T) value.toString();
        }
        
        if (targetType == int.class || targetType == Integer.class) {
            if (value instanceof Number) {
                return (T) Integer.valueOf(((Number) value).intValue());
            }
            return (T) Integer.valueOf(value.toString());
        }
        
        if (targetType == long.class || targetType == Long.class) {
            if (value instanceof Number) {
                return (T) Long.valueOf(((Number) value).longValue());
            }
            return (T) Long.valueOf(value.toString());
        }
        
        if (targetType == double.class || targetType == Double.class) {
            if (value instanceof Number) {
                return (T) Double.valueOf(((Number) value).doubleValue());
            }
            return (T) Double.valueOf(value.toString());
        }
        
        if (targetType == boolean.class || targetType == Boolean.class) {
            if (value instanceof Boolean) {
                return (T) value;
            }
            return (T) Boolean.valueOf(value.toString());
        }
        
        if (value instanceof String) {
            String str = (String) value;
            if (str.startsWith("{") || str.startsWith("[")) {
                try {
                    return JsonSerde.fromJson(str, targetType);
                } catch (Exception ignored) {
                }
            }
        }
        
        return (T) value;
    }

    public static String getCollectionName(Class<?> clazz) {
        Entity annotation = clazz.getAnnotation(Entity.class);
        return annotation != null && !annotation.value().isEmpty() 
            ? annotation.value() 
            : clazz.getSimpleName().toLowerCase();
    }

    public static String getIdFieldName(Class<?> clazz) {
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Id.class)) {
                String colValue = field.getAnnotation(Id.class).value();
                return !colValue.isEmpty() ? colValue : field.getName();
            }
        }
        return "id";
    }

    public static Object getIdValue(Object entity) {
        Class<?> clazz = entity.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Id.class)) {
                field.setAccessible(true);
                try {
                    return field.get(entity);
                } catch (IllegalAccessException e) {
                    return null;
                }
            }
        }
        return null;
    }
}
