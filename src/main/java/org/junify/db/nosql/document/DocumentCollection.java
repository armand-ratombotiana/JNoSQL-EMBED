package org.junify.db.nosql.document;

import org.junify.db.core.cache.QueryResultCache;
import org.junify.db.event.EventBus;
import org.junify.db.index.SecondaryIndex;
import org.junify.db.core.metrics.DatabaseMetrics;
import org.junify.db.storage.StorageEngine;
import org.junify.db.core.util.JsonSerde;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DocumentCollection {

    private final String name;
    private final StorageEngine engine;
    private final EventBus eventBus;
    private final DatabaseMetrics metrics;
    private final Map<String, SecondaryIndex> indexes;
    private final QueryResultCache cache;
    private final Path dataDir;

    public DocumentCollection(String name, StorageEngine engine, EventBus eventBus, DatabaseMetrics metrics) {
        this(name, engine, eventBus, metrics, null, null);
    }

    public DocumentCollection(String name, StorageEngine engine, EventBus eventBus, DatabaseMetrics metrics, QueryResultCache cache) {
        this(name, engine, eventBus, metrics, cache, null);
    }

    public DocumentCollection(String name, StorageEngine engine, EventBus eventBus, DatabaseMetrics metrics, QueryResultCache cache, Path dataDir) {
        this.name = name;
        this.engine = engine;
        this.eventBus = eventBus;
        this.metrics = metrics;
        this.indexes = new ConcurrentHashMap<>();
        this.cache = cache;
        this.dataDir = dataDir;
    }

    public String name() {
        return name;
    }

    public QueryResultCache cache() {
        return cache;
    }

    public SecondaryIndex createIndex(String field) {
        var idx = new SecondaryIndex(name, field);
        indexes.put(field, idx);
        for (var doc : findAll()) {
            idx.add(doc);
        }
        saveIndexes();
        return idx;
    }

    public void loadIndexes() {
        if (dataDir == null) return;
        
        var indexFile = dataDir.resolve(".indexes");
        if (!Files.exists(indexFile)) return;
        
        try {
            var content = Files.readString(indexFile);
            var map = JsonSerde.fromJson(content, Map.class);
            var collectionIndexes = (Map<?, ?>) map.get(name);
            if (collectionIndexes == null) return;
            
            for (var entry : collectionIndexes.entrySet()) {
                var field = (String) entry.getKey();
                var idxJson = (String) entry.getValue();
                var idx = SecondaryIndex.fromJson(idxJson);
                indexes.put(field, idx);
            }
        } catch (IOException e) {
            System.err.println("Failed to load indexes: " + e.getMessage());
        }
    }

    public void saveIndexes() {
        if (dataDir == null) return;
        
        var indexFile = dataDir.resolve(".indexes");
        Map<String, String> collectionIndexes = new HashMap<>();
        
        for (var entry : indexes.entrySet()) {
            collectionIndexes.put(entry.getKey(), entry.getValue().toJson());
        }
        
        Map<String, Map<String, String>> allIndexes = new HashMap<>();
        allIndexes.put(name, collectionIndexes);
        
        try {
            var json = JsonSerde.toJson(allIndexes);
            Files.writeString(indexFile, json);
        } catch (IOException e) {
            System.err.println("Failed to save indexes: " + e.getMessage());
        }
    }

    public SecondaryIndex getIndex(String field) {
        return indexes.get(field);
    }

    public Map<String, SecondaryIndex> getIndexes() {
        return Map.copyOf(indexes);
    }

    public Document insert(Document doc) {
        return insert(doc, -1);
    }

    public Document insert(Document doc, long ttlSeconds) {
        eventBus.emit(EventBus.EventType.BEFORE_INSERT, name, doc);
        if (doc.id() == null) {
            doc.id(UUID.randomUUID().toString());
        }
        if (ttlSeconds > 0) {
            doc.expiresAt(System.currentTimeMillis() + (ttlSeconds * 1000));
        }
        engine.put(name, doc.id(), doc.toJson());
        for (var idx : indexes.values()) {
            idx.add(doc);
        }
        if (cache != null) {
            cache.invalidatePattern(name);
        }
        metrics.recordInsert();
        metrics.updateCollectionSize(name, count());
        eventBus.emit(EventBus.EventType.AFTER_INSERT, name, doc);
        return doc;
    }

    public List<Document> insertAll(List<Document> docs) {
        return docs.stream().map(this::insert).collect(Collectors.toList());
    }

    public Document findById(String id) {
        metrics.recordRead();
        var json = engine.get(name, id);
        return json != null ? Document.fromJson(json) : null;
    }

    public List<Document> findAll() {
        if (cache != null) {
            var cached = cache.get(name + ":findAll");
            if (cached != null) {
                return cached;
            }
        }
        var results = engine.scan(name).stream()
                .map(Document::fromJson)
                .collect(Collectors.toList());
        if (cache != null) {
            cache.put(name + ":findAll", results);
        }
        return results;
    }

    public List<Document> findByIds(List<String> ids) {
        metrics.recordRead();
        var results = new ArrayList<Document>();
        for (var id : ids) {
            var json = engine.get(name, id);
            if (json != null) {
                results.add(Document.fromJson(json));
            }
        }
        return results;
    }

    public List<Document> find(Query query) {
        metrics.recordQuery();
        
        String cacheKey = cache != null ? QueryResultCache.cacheKey(name, query.toString()) : null;
        if (cacheKey != null) {
            var cached = cache.get(cacheKey);
            if (cached != null) {
                return cached;
            }
        }
        
        List<Document> results;

        if (shouldUseIndex(query)) {
            results = findWithIndex(query);
        } else {
            results = engine.scan(name).stream()
                    .map(Document::fromJson)
                    .filter(query.docPredicate())
                    .collect(Collectors.toList());
        }

        if (query.sortOrder() != Query.SortOrder.NONE && query.sortField() != null) {
            var sf = query.sortField();
            Comparator<Document> cmp = (a, b) -> {
                var va = toComparable(a.getRaw(sf));
                var vb = toComparable(b.getRaw(sf));
                if (va == null && vb == null) return 0;
                if (va == null) return -1;
                if (vb == null) return 1;
                return ((Comparable) va).compareTo(vb);
            };
            if (query.sortOrder() == Query.SortOrder.DESC) {
                cmp = cmp.reversed();
            }
            results.sort(cmp);
        }

        int offset = query.offset();
        int limit = query.limit();
        if (offset > 0 || limit < Integer.MAX_VALUE) {
            int from = Math.min(offset, results.size());
            int to = (limit == Integer.MAX_VALUE) ? results.size() : Math.min(from + limit, results.size());
            var paged = results.subList(from, to);
            if (cacheKey != null) {
                cache.put(cacheKey, paged);
            }
            return paged;
        }

        if (cacheKey != null) {
            cache.put(cacheKey, results);
        }
        return results;
    }

    private boolean shouldUseIndex(Query query) {
        var pred = query.docPredicate();
        if (indexes.isEmpty()) return false;
        return true;
    }

    private List<Document> findWithIndex(Query query) {
        var pred = query.docPredicate();
        for (var entry : indexes.entrySet()) {
            var idx = entry.getValue();
            var results = new ArrayList<Document>();
            var ids = idx.allValues();
            for (var id : ids) {
                var doc = findById(id);
                if (doc != null && pred.test(doc)) {
                    results.add(doc);
                }
            }
            return results;
        }
        return findAll().stream().filter(pred).collect(Collectors.toList());
    }

    public Document findOne(Query query) {
        var results = find(query.limit(1));
        return results.isEmpty() ? null : results.get(0);
    }

    public Document update(Document doc) {
        eventBus.emit(EventBus.EventType.BEFORE_UPDATE, name, doc);
        if (doc.id() == null) {
            throw new IllegalArgumentException("Document must have an id to update");
        }
        var oldDoc = findById(doc.id());
        if (oldDoc == null) {
            throw new IllegalArgumentException("Document not found: " + doc.id());
        }
        engine.put(name, doc.id(), doc.toJson());
        for (var idx : indexes.values()) {
            idx.update(oldDoc, doc);
        }
        if (cache != null) {
            cache.invalidatePattern(name);
        }
        metrics.recordUpdate();
        eventBus.emit(EventBus.EventType.AFTER_UPDATE, name, doc);
        return doc;
    }

    public Document upsert(Document doc) {
        if (doc.id() != null && engine.exists(name, doc.id())) {
            return update(doc);
        }
        return insert(doc);
    }

    public boolean deleteById(String id) {
        if (engine.exists(name, id)) {
            var oldDoc = findById(id);
            eventBus.emit(EventBus.EventType.BEFORE_DELETE, name, id);
            engine.delete(name, id);
            if (oldDoc != null) {
                for (var idx : indexes.values()) {
                    idx.remove(oldDoc);
                }
            }
            if (cache != null) {
                cache.invalidatePattern(name);
            }
            metrics.recordDelete();
            metrics.updateCollectionSize(name, count());
            eventBus.emit(EventBus.EventType.AFTER_DELETE, name, id);
            return true;
        }
        return false;
    }

    public long deleteAll(Query query) {
        var docs = find(query.limit(Integer.MAX_VALUE));
        for (var d : docs) {
            engine.delete(name, d.id());
            for ( var idx : indexes.values()) {
                idx.remove(d);
            }
            metrics.recordDelete();
        }
        metrics.updateCollectionSize(name, count());
        return docs.size();
    }

    public long bulkDelete(List<String> ids) {
        long deleted = 0;
        for (var id : ids) {
            if (deleteById(id)) {
                deleted++;
            }
        }
        return deleted;
    }

    public long count() {
        return engine.keys(name).size();
    }

    public long count(Query query) {
        return find(query).size();
    }

    public long cleanupExpired() {
        long cleaned = 0;
        for (var doc : findAll()) {
            if (doc.isExpired()) {
                deleteById(doc.id());
                cleaned++;
            }
        }
        return cleaned;
    }

    public boolean exists(String id) {
        return engine.exists(name, id);
    }

    public void clear() {
        for (var doc : findAll()) {
            engine.delete(name, doc.id());
        }
        indexes.clear();
    }

    public Map<String, Object> stats() {
        return Map.of(
                "collection", name,
                "count", count(),
                "indexCount", indexes.size(),
                "storageEngine", engine.name(),
                "engineStats", engine.stats()
        );
    }

    @SuppressWarnings("unchecked")
    private static Comparable<?> toComparable(Object value) {
        if (value == null) return null;
        if (value instanceof Comparable c) return c;
        return value.toString();
    }
}
