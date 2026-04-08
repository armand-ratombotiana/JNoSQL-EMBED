package org.jnosql.embed.document;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class Query {

    private final Predicate<Document> docPredicate;
    private SortOrder sortOrder;
    private String sortField;
    private int limit = Integer.MAX_VALUE;
    private int offset = 0;
    private Set<QueryHint> hints = new java.util.HashSet<>();
    private String forceIndexName;

    private Query(Predicate<Document> docPredicate) {
        this.docPredicate = docPredicate;
        this.sortOrder = SortOrder.NONE;
        this.sortField = null;
    }

    public static Query all() {
        return new Query(doc -> true);
    }

    public static Query eq(String field, Object value) {
        return new Query(doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            if (v == null) return value == null;
            return v.equals(value);
        });
    }

    public static Query contains(String field, String substring) {
        return new Query(doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            return v != null && v.toString().contains(substring);
        });
    }

    public static Query gt(String field, Number value) {
        return new Query(doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            if (v instanceof Number n) {
                return n.doubleValue() > value.doubleValue();
            }
            return false;
        });
    }

    public static Query gte(String field, Number value) {
        return new Query(doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            if (v instanceof Number n) {
                return n.doubleValue() >= value.doubleValue();
            }
            return false;
        });
    }

    public static Query lt(String field, Number value) {
        return new Query(doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            if (v instanceof Number n) {
                return n.doubleValue() < value.doubleValue();
            }
            return false;
        });
    }

    public static Query lte(String field, Number value) {
        return new Query(doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            if (v instanceof Number n) {
                return n.doubleValue() <= value.doubleValue();
            }
            return false;
        });
    }

    public static Query in(String field, List<?> values) {
        return new Query(doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            return values.contains(v);
        });
    }

    public static Query ne(String field, Object value) {
        return new Query(doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            if (v == null) return value != null;
            return !v.equals(value);
        });
    }

    public static Query exists(String field) {
        return new Query(doc -> doc.has(field));
    }

    public static Query regex(String field, String pattern) {
        var regex = java.util.regex.Pattern.compile(pattern);
        return new Query(doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            return v != null && regex.matcher(v.toString()).find();
        });
    }

    public Query and(Query other) {
        var q = new Query(this.docPredicate.and(other.docPredicate));
        q.sortOrder = this.sortOrder;
        q.sortField = this.sortField;
        q.limit = this.limit;
        q.offset = this.offset;
        return q;
    }

    public Query or(Query other) {
        var q = new Query(this.docPredicate.or(other.docPredicate));
        q.sortOrder = this.sortOrder;
        q.sortField = this.sortField;
        q.limit = this.limit;
        q.offset = this.offset;
        return q;
    }

    public Query sortBy(String field, SortOrder order) {
        this.sortField = field;
        this.sortOrder = order;
        return this;
    }

    public Query sortByAsc(String field) {
        return sortBy(field, SortOrder.ASC);
    }

    public Query sortByDesc(String field) {
        return sortBy(field, SortOrder.DESC);
    }

    public Query limit(int limit) {
        this.limit = limit;
        return this;
    }

    public Query offset(int offset) {
        this.offset = offset;
        return this;
    }

    public Query page(int page, int pageSize) {
        this.offset = page * pageSize;
        this.limit = pageSize;
        return this;
    }

    public Query withHint(QueryHint hint) {
        this.hints.add(hint);
        return this;
    }

    public Query withHint(String indexName) {
        this.hints.add(QueryHint.FORCE_INDEX);
        this.forceIndexName = indexName;
        return this;
    }

    public Query noIndex() {
        this.hints.add(QueryHint.NO_INDEX);
        return this;
    }

    public Query parallel() {
        this.hints.add(QueryHint.PARALLEL);
        return this;
    }

    public boolean hasHint(QueryHint hint) {
        return hints.contains(hint);
    }

    public String getForceIndexName() {
        return forceIndexName;
    }

    public Set<QueryHint> getHints() {
        return Set.copyOf(hints);
    }

    public Predicate<Document> docPredicate() {
        return docPredicate;
    }

    SortOrder sortOrder() {
        return sortOrder;
    }

    String sortField() {
        return sortField;
    }

    int limit() {
        return limit;
    }

    public List<Document> apply(List<Document> documents) {
        return documents.stream().filter(docPredicate).collect(java.util.stream.Collectors.toList());
    }

    int offset() {
        return offset;
    }

    @Override
    public String toString() {
        return "Query{pred=" + docPredicate + ", sort=" + sortOrder + " " + sortField + 
               ", limit=" + limit + ", offset=" + offset + ", hints=" + hints + "}";
    }

    public enum SortOrder { ASC, DESC, NONE }

    public enum QueryHint {
        FORCE_INDEX,
        NO_INDEX,
        PARALLEL,
        CACHE_RESULT,
        NO_CACHE
    }
}
