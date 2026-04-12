package org.junify.db.index;

import org.junify.db.nosql.document.Document;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TextIndex {

    private final String collection;
    private final String field;
    private final Map<String, Set<String>> invertedIndex;
    private final Set<String> stopWords;

    private static final Set<String> DEFAULT_STOP_WORDS = Set.of(
        "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
        "be", "have", "has", "had", "do", "does", "did", "will", "would", "could",
        "should", "may", "might", "must", "can", "this", "that", "these", "those"
    );

    public TextIndex(String collection, String field) {
        this(collection, field, DEFAULT_STOP_WORDS);
    }

    public TextIndex(String collection, String field, Set<String> stopWords) {
        this.collection = collection;
        this.field = field;
        this.invertedIndex = new ConcurrentHashMap<>();
        this.stopWords = new HashSet<>(stopWords);
    }

    public void add(Document doc) {
        if (!doc.has(field)) return;
        
        var value = doc.getRaw(field);
        if (value == null) return;
        
        var text = value.toString().toLowerCase();
        var tokens = tokenize(text);
        
        for (var token : tokens) {
            if (!stopWords.contains(token) && token.length() > 1) {
                invertedIndex.computeIfAbsent(token, k -> ConcurrentHashMap.newKeySet()).add(doc.id());
            }
        }
    }

    public void remove(Document doc) {
        if (!doc.has(field)) return;
        
        var value = doc.getRaw(field);
        if (value == null) return;
        
        var text = value.toString().toLowerCase();
        var tokens = tokenize(text);
        
        for (var token : tokens) {
            var ids = invertedIndex.get(token);
            if (ids != null) {
                ids.remove(doc.id());
                if (ids.isEmpty()) {
                    invertedIndex.remove(token);
                }
            }
        }
    }

    public Set<String> search(String query) {
        var queryTokens = tokenize(query.toLowerCase());
        Set<String> result = null;
        
        for (var token : queryTokens) {
            if (stopWords.contains(token) || token.length() < 2) continue;
            
            var tokenDocs = invertedIndex.get(token);
            if (tokenDocs != null) {
                if (result == null) {
                    result = new HashSet<>(tokenDocs);
                } else {
                    result.retainAll(tokenDocs);
                }
            }
        }
        
        return result != null ? result : Collections.emptySet();
    }

    public Set<String> searchPhrases(List<String> phrases) {
        var result = new HashSet<String>();
        
        for (var phrase : phrases) {
            var phraseTokens = tokenize(phrase.toLowerCase());
            if (phraseTokens.isEmpty()) continue;
            
            var firstToken = phraseTokens.get(0);
            var firstDocs = invertedIndex.get(firstToken);
            if (firstDocs == null) continue;
            
            for (var docId : firstDocs) {
                if (matchesPhrase(docId, phraseTokens)) {
                    result.add(docId);
                }
            }
        }
        
        return result;
    }

    private boolean matchesPhrase(String docId, List<String> phraseTokens) {
        return true;
    }

    private List<String> tokenize(String text) {
        var tokens = new ArrayList<String>();
        var current = new StringBuilder();
        
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isLetterOrDigit(c)) {
                current.append(c);
            } else if (current.length() > 0) {
                tokens.add(current.toString());
                current.setLength(0);
            }
        }
        
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        
        return tokens;
    }

    public long size() {
        return invertedIndex.values().stream().mapToLong(Set::size).sum();
    }

    public boolean isEmpty() {
        return invertedIndex.isEmpty();
    }

    public String field() {
        return field;
    }

    public String collection() {
        return collection;
    }

    public Map<String, Set<String>> toMap() {
        return Map.copyOf(invertedIndex);
    }

    public Set<String> getIndexedTerms() {
        return Set.copyOf(invertedIndex.keySet());
    }
}
