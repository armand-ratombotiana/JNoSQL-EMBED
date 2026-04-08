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

    public SecondaryIndex(String collection, String field, Map<Object, Set<String>> index) {
        this.collection = collection;
        this.field = field;
        this.index = new ConcurrentHashMap<>(index);
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

    public Map<Object, Set<String>> toMap() {
        return Map.copyOf(index);
    }

    public String toJson() {
        var sb = new StringBuilder();
        sb.append("{\"collection\":\"").append(collection).append("\",");
        sb.append("\"field\":\"").append(field).append("\",");
        sb.append("\"index\":{");
        var first = true;
        for (var entry : index.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("\"").append(entry.getKey()).append("\":[");
            var firstId = true;
            for (var id : entry.getValue()) {
                if (!firstId) sb.append(",");
                firstId = false;
                sb.append("\"").append(id).append("\"");
            }
            sb.append("]");
        }
        sb.append("}}");
        return sb.toString();
    }

    public static SecondaryIndex fromJson(String json) {
        var map = org.jnosql.embed.util.JsonSerde.fromJson(json, Map.class);
        var coll = (String) map.get("collection");
        var fld = (String) map.get("field");
        var idxMap = (Map<?, ?>) map.get("index");
        
        Map<Object, Set<String>> index = new ConcurrentHashMap<>();
        for (var entry : idxMap.entrySet()) {
            var key = entry.getKey();
            var ids = (List<?>) entry.getValue();
            Set<String> idSet = ConcurrentHashMap.newKeySet();
            for (var id : ids) {
                idSet.add((String) id);
            }
            index.put(key, idSet);
        }
        
        return new SecondaryIndex(coll, fld, index);
    }
}
