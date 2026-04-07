package org.jnosql.embed.document;

import org.jnosql.embed.storage.StorageEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
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
        return engine.scan(name).stream()
                .map(Document::fromJson)
                .filter(query.docPredicate())
                .collect(Collectors.toList());
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

    public boolean deleteById(String id) {
        if (engine.exists(name, id)) {
            engine.delete(name, id);
            return true;
        }
        return false;
    }

    public long deleteAll(Query query) {
        var docs = find(query);
        for (var d : docs) {
            engine.delete(name, d.id());
        }
        return docs.size();
    }

    public long count() {
        return engine.scan(name).size();
    }

    public boolean exists(String id) {
        return engine.exists(name, id);
    }

    public void clear() {
        for (var doc : findAll()) {
            engine.delete(name, doc.id());
        }
    }
}
