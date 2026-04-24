package org.junify.db.spring.data;

import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.kv.KeyValueBucket;

import java.util.*;
import java.util.function.Function;

import org.springframework.data.annotation.Id;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentEntity;
import org.springframework.data.keyvalue.core.mapping.context.KeyValueMappingContext;
import org.springframework.data.keyvalue.repository.KeyValueRepository;
import org.springframework.data.keyvalue.repository.SimpleKeyValueRepository;

public class JunifyDBTemplate {

    private final JunifyDB db;
    private final KeyValueTemplate template;
    private final KeyValueMappingContext<?, ?> context;

    public JunifyDBTemplate(JunifyDB db) {
        this.db = db;
        this.context = new KeyValueMappingContext<>();
        this.template = new KeyValueTemplate(
            new JunifyDBKeyValueAdapter(db),
            context
        );
    }

    public <T> JunifyDBRepository<T, ?> repository(Class<T> entity) {
        return new JunifyDBRepository<>(entity, template, context);
    }

    public DocumentCollection documentCollection(String name) {
        return db.documentCollection(name);
    }

    public KeyValueBucket keyValueBucket(String name) {
        return db.keyValueBucket(name);
    }

    public void destroy() throws Exception {
        db.close();
    }

    public static class JunifyDBKeyValueAdapter implements org.springframework.data.keyvalue.core.KeyValueAdapter {

        private final JunifyDB db;

        public JunifyDBKeyValueAdapter(JunifyDB db) {
            this.db = db;
        }

        @Override
        public Object get(Object id, String keySpace) {
            return db.keyValueBucket(keySpace).get(String.valueOf(id));
        }

        @Override
        public Iterable<?> getAll(Iterable<?> ids, String keySpace) {
            var bucket = db.keyValueBucket(keySpace);
            var results = new ArrayList<>();
            for (var id : ids) {
                var value = bucket.get(String.valueOf(id));
                if (value != null) {
                    results.add(value);
                }
            }
            return results;
        }

        @Override
        public void put(Object id, Object entity, String keySpace) {
            db.keyValueBucket(keySpace).put(String.valueOf(id), serialize(entity));
        }

        @Override
        public void putAll(Map<?, ?> entries, String keySpace) {
            var bucket = db.keyValueBucket(keySpace);
            for (var entry : entries.entrySet()) {
                bucket.put(String.valueOf(entry.getKey()), serialize(entry.getValue()));
            }
        }

        @Override
        public boolean delete(Object id, String keySpace) {
            var bucket = db.keyValueBucket(keySpace);
            var existed = bucket.get(String.valueOf(id)) != null;
            bucket.delete(String.valueOf(id));
            return existed;
        }

        @Override
        public long delete(Iterable<?> ids, String keySpace) {
            var bucket = db.keyValueBucket(keySpace);
            long count = 0;
            for (var id : ids) {
                if (bucket.get(String.valueOf(id)) != null) {
                    bucket.delete(String.valueOf(id));
                    count++;
                }
            }
            return count;
        }

        @Override
        public void deleteAll(String keySpace) {
            db.keyValueBucket(keySpace).clear();
        }

        @Override
        public long count(String keySpace) {
            return db.keyValueBucket(keySpace).size();
        }

        @Override
        public Iterable<Object> execute(Function fn) {
            return Collections.emptyList();
        }

        @Override
        public void close() {}

        private String serialize(Object entity) {
            if (entity instanceof String) {
                return (String) entity;
            }
            return org.junify.db.core.util.JsonSerde.toJson(entity);
        }
    }

    public interface JunifyDBRepository<T, ID> extends KeyValueRepository<T, ID> {
        @Override
        T save(T entity);

        @Override
        Optional<T> findById(ID id);

        @Override
        void delete(T entity);

        @Override
        void deleteById(ID id);

        @Override
        Iterable<T> findAll();

        @Override
        long count();
    }

    public static class JunifyDBRepositoryImpl<T, ID> extends SimpleKeyValueRepository<T, ID>
            implements JunifyDBRepository<T, ID> {

        private final Class<T> entityType;
        private final KeyValueTemplate template;
        private final KeyValueMappingContext<?, ?> context;

        public JunifyDBRepositoryImpl(Class<T> entityType, KeyValueTemplate template,
                                  KeyValueMappingContext<?, ?> context) {
            super(template, context.getPersistentEntity(entityType));
            this.entityType = entityType;
            this.template = template;
            this.context = context;
        }

        @Override
        public T save(T entity) {
            template.insert(entity);
            return entity;
        }

        @Override
        public Optional<T> findById(ID id) {
            return template.findById(id, entityType);
        }

        @Override
        public Iterable<T> findAll() {
            return template.findAll(entityType);
        }

        @Override
        public long count() {
            return template.count(entityType);
        }

        @Override
        public void delete(T entity) {
            template.delete(entity);
        }

        @Override
        public void deleteById(ID id) {
            template.deleteById(id, entityType);
        }
    }
}