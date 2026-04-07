package org.jnosql.embed.document;

import org.jnosql.embed.storage.StorageEngine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class DocumentCollection {

    private final String name;
    private final StorageEngine engine;

    public DocumentCollection(String name, StorageEngine engine) {
        this.name = name;
        this.engine = engine;
    }

    public String name() {
        return name;
    }

    public Document insert(Document doc) {
        if (doc.id() == null) {
            doc.id(UUID.randomUUID().toString());
        }
        engine.put(name, doc.id(), doc.toJson());
        return doc;
    }

    public List<Document> insertAll(List<Document> docs) {
        return docs.stream().map(this::insert).collect(Collectors.toList());
    }

    public Document findById(String id) {
        var json = engine.get(name, id);
        return json != null ? Document.fromJson(json) : null;
    }

    public List<Document> findAll() {
        return engine.scan(name).stream()
                .map(Document::fromJson)
                .collect(Collectors.toList());
    }

    public List<Document> find(Query query) {
        var results = engine.scan(name).stream()
                .map(Document::fromJson)
                .filter(query.docPredicate())
                .collect(Collectors.toList());

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
            return results.subList(from, to);
        }

        return results;
    }

    public Document findOne(Query query) {
        var results = find(query.limit(1));
        return results.isEmpty() ? null : results.get(0);
    }

    public Document update(Document doc) {
        if (doc.id() == null) {
            throw new IllegalArgumentException("Document must have an id to update");
        }
        if (!engine.exists(name, doc.id())) {
            throw new IllegalArgumentException("Document not found: " + doc.id());
        }
        engine.put(name, doc.id(), doc.toJson());
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
            engine.delete(name, id);
            return true;
        }
        return false;
    }

    public long deleteAll(Query query) {
        var docs = find(query.limit(Integer.MAX_VALUE));
        for (var d : docs) {
            engine.delete(name, d.id());
        }
        return docs.size();
    }

    public long count() {
        return engine.scan(name).size();
    }

    public long count(Query query) {
        return engine.scan(name).stream()
                .map(Document::fromJson)
                .filter(query.docPredicate())
                .count();
    }

    public boolean exists(String id) {
        return engine.exists(name, id);
    }

    public void clear() {
        for (var doc : findAll()) {
            engine.delete(name, doc.id());
        }
    }

    public Map<String, Object> stats() {
        return Map.of(
                "collection", name,
                "count", count(),
                "storageEngine", engine.getClass().getSimpleName()
        );
    }

    @SuppressWarnings("unchecked")
    private static Comparable<?> toComparable(Object value) {
        if (value == null) return null;
        if (value instanceof Comparable c) return c;
        return value.toString();
    }
}
