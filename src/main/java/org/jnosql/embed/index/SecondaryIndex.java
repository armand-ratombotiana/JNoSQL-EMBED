package org.jnosql.embed.index;

import org.jnosql.embed.document.Document;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SecondaryIndex {

    private final String collection;
    private final String field;
    private final Map<Object, Set<String>> index;

    public SecondaryIndex(String collection, String field) {
        this.collection = collection;
        this.field = field;
        this.index = new ConcurrentHashMap<>();
    }

    public void add(Document doc) {
        if (!doc.has(field)) return;
        var value = doc.getRaw(field);
        index.computeIfAbsent(value, k -> ConcurrentHashMap.newKeySet()).add(doc.id());
    }

    public void remove(Document doc) {
        if (!doc.has(field)) return;
        var value = doc.getRaw(field);
        var ids = index.get(value);
        if (ids != null) {
            ids.remove(doc.id());
            if (ids.isEmpty()) {
                index.remove(value);
            }
        }
    }

    public void update(Document oldDoc, Document newDoc) {
        remove(oldDoc);
        add(newDoc);
    }

    public Set<String> lookup(Object value) {
        return index.getOrDefault(value, Collections.emptySet());
    }

    public Set<String> allValues() {
        return index.keySet().stream()
                .flatMap(v -> index.get(v).stream())
                .collect(java.util.stream.Collectors.toSet());
    }

    public long size() {
        return index.values().stream().mapToLong(Set::size).sum();
    }

    public boolean isEmpty() {
        return index.isEmpty();
    }

    public String field() {
        return field;
    }

    public String collection() {
        return collection;
    }
}
