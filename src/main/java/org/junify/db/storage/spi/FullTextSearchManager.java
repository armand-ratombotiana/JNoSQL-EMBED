package org.junify.db.storage.spi;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class FullTextSearchManager {

    private final H2StorageEngine engine;
    private final Map<String, FTSIndex> indexes;
    private static final int DEFAULT_K = 10;

    public FullTextSearchManager(H2StorageEngine engine) {
        this.engine = engine;
        this.indexes = new ConcurrentHashMap<>();
        initializeSchema();
    }

    private void initializeSchema() {
        engine.executeSql("CREATE TABLE IF NOT EXISTS fts_indexes (" +
            "index_name VARCHAR(255) PRIMARY KEY, " +
            "table_name VARCHAR(255), " +
            "columns VARCHAR(1000), " +
            "created_at BIGINT)"
        );
    }

    public SqlResult createIndex(String indexName, String tableName, String... columns) {
        var existing = indexes.get(indexName);
        if (existing != null) {
            return new SqlResult(false, null, 0, "Index already exists: " + indexName);
        }
        
        var colList = String.join(", ", columns);
        var index = new FTSIndex(indexName, tableName, Arrays.asList(columns));
        indexes.put(indexName, index);
        
        engine.executeSql("INSERT INTO fts_indexes VALUES ('" + indexName + "', '" + 
            tableName + "', '" + colList + "', " + System.currentTimeMillis() + ")");
        
        rebuildIndex(indexName);
        
        return new SqlResult(true, null, 0, "FTS index created: " + indexName);
    }

    public SqlResult dropIndex(String indexName) {
        indexes.remove(indexName);
        return engine.executeSql("DELETE FROM fts_indexes WHERE index_name = '" + indexName + "'");
    }

    public SqlResult rebuildIndex(String indexName) {
        var index = indexes.get(indexName);
        if (index == null) {
            return new SqlResult(false, null, 0, "Index not found: " + indexName);
        }
        
        var result = engine.executeSql("SELECT " + String.join(", ", index.columns) + 
            ", " + index.pkColumn() + " FROM " + index.tableName());
        
        if (!result.success() || result.rows() == null) {
            return new SqlResult(false, null, 0, "No data to index");
        }
        
        index.clear();
        
        for (var row : result.rows()) {
            var pk = String.valueOf(row.get(index.pkColumn()));
            var text = new StringBuilder();
            
            for (var col : index.columns) {
                var val = row.get(col);
                if (val != null) {
                    text.append(val).append(" ");
                }
            }
            
            index.addDocument(pk, tokenize(text.toString()));
        }
        
        return new SqlResult(true, null, index.documentCount(), 
            "Indexed " + index.documentCount() + " documents");
    }

    public FTSSearchResult search(String indexName, String query, int k) {
        var index = indexes.get(indexName);
        if (index == null) {
            return new FTSSearchResult(Collections.emptyList(), 0, "Index not found");
        }
        
        var queryTokens = tokenize(query);
        var results = new ArrayList<FTSScore>();
        
        for (var doc : index.documents()) {
            var score = calculateScore(queryTokens, doc.tokens());
            if (score > 0) {
                results.add(new FTSScore(doc.id(), score));
            }
        }
        
        results.sort((a, b) -> Double.compare(b.score, a.score));
        
        var topK = results.stream().limit(k > 0 ? k : DEFAULT_K).toList();
        
        return new FTSSearchResult(topK, topK.size(), "OK");
    }

    public FTSSearchResult searchWithHighlight(String indexName, String query, int k) {
        var results = search(indexName, query, k);
        var index = indexes.get(indexName);
        
        if (index == null) {
            return results;
        }
        
        var highlighted = new ArrayList<FTSScore>();
        
        for (var r : results.results()) {
            var doc = index.getDocument(r.id());
            if (doc != null) {
                var highlightedText = highlight(doc.text(), query);
                highlighted.add(new FTSScore(r.id(), r.score(), highlightedText));
            }
        }
        
        return new FTSSearchResult(highlighted, highlighted.size(), "OK");
    }

    private List<String> tokenize(String text) {
        if (text == null || text.isEmpty()) {
            return Collections.emptyList();
        }
        
        return Arrays.stream(text.toLowerCase().split("[\\s.,;:!?'\"()]+"))
            .filter(s -> s.length() > 2)
            .filter(s -> !isStopWord(s))
            .distinct()
            .collect(Collectors.toList());
    }

    private boolean isStopWord(String word) {
        return Set.of("the", "a", "an", "and", "or", "but", "in", "on", "at", 
            "to", "for", "of", "with", "by", "from").contains(word);
    }

    private double calculateScore(List<String> queryTokens, Map<String, Integer> docTokens) {
        if (queryTokens.isEmpty() || docTokens.isEmpty()) {
            return 0;
        }
        
        var score = 0.0;
        
        for (var token : queryTokens) {
            if (docTokens.containsKey(token)) {
                var tf = docTokens.get(token);
                score += 1 + Math.log(tf);
            }
        }
        
        var docLength = docTokens.values().stream().mapToInt(Integer::intValue).sum();
        if (docLength > 0) {
            score = score / Math.sqrt(docLength);
        }
        
        return score;
    }

    private String highlight(String text, String query) {
        var tokens = tokenize(query);
        var result = text;
        
        for (var token : tokens) {
            var pattern = "(?i)(" + token + ")";
            result = result.replaceAll(pattern, "<mark>$1</mark>");
        }
        
        return result;
    }

    public List<String> getIndexes() {
        return new ArrayList<>(indexes.keySet());
    }

    public SqlResult getIndexInfo(String indexName) {
        var index = indexes.get(indexName);
        if (index == null) {
            return new SqlResult(false, null, 0, "Index not found");
        }
        
        var info = Map.of(
            "indexName", indexName,
            "tableName", index.tableName(),
            "columns", index.columns().toString(),
            "documentCount", index.documentCount()
        );
        
        return new SqlResult(true, List.of("metric", "value"), 4, "OK",
            info.entrySet().stream()
                .map(e -> Map.of("metric", e.getKey(), "value", String.valueOf(e.getValue())))
                .toList(),
            List.of("metric", "value"));
    }

    private class FTSIndex {
        final String name;
        final String tableName;
        final List<String> columns;
        final ConcurrentHashMap<String, Map<String, Integer>> documents;

        FTSIndex(String name, String tableName, List<String> columns) {
            this.name = name;
            this.tableName = tableName;
            this.columns = columns;
            this.documents = new ConcurrentHashMap<>();
        }

        String tableName() { return tableName; }
        List<String> columns() { return columns; }

        String pkColumn() { return "id"; }

        void addDocument(String id, List<String> tokens) {
            var tf = new ConcurrentHashMap<String, Integer>();
            for (var token : tokens) {
                tf.merge(token, 1, Integer::sum);
            }
            documents.put(id, tf);
        }

        Document getDocument(String id) {
            var tf = documents.get(id);
            return tf != null ? new Document(id, tf) : null;
        }

        void clear() { documents.clear(); }
        int documentCount() { return documents.size(); }
        
        List<Document> documents() {
            return documents.entrySet().stream()
                .map(e -> new Document(e.getKey(), e.getValue()))
                .toList();
        }

        record Document(String id, Map<String, Integer> tokens) {
            String text() { return String.join(" ", tokens.keySet()); }
        }
    }

    public record FTSScore(String id, double score, String highlighted) {
        public FTSScore(String id, double score) {
            this(id, score, null);
        }
    }

    public record FTSSearchResult(List<FTSScore> results, int count, String status) {}

    public record SqlResult(boolean success, List<String> columns, int affected, String message,
                     List<Map<String, Object>> rows, List<String> allColumns) {
        public boolean success() { return success; }
    }
}