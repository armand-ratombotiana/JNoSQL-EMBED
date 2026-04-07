package org.jnosql.embed.document;

import java.util.function.Predicate;

public class Query {

    private final Predicate<Document> docPredicate;

    private Query(Predicate<Document> docPredicate) {
        this.docPredicate = docPredicate;
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

    public static Query regex(String field, String pattern) {
        var regex = java.util.regex.Pattern.compile(pattern);
        return new Query(doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            return v != null && regex.matcher(v.toString()).find();
        });
    }

    public Query and(Query other) {
        return new Query(this.docPredicate.and(other.docPredicate));
    }

    public Query or(Query other) {
        return new Query(this.docPredicate.or(other.docPredicate));
    }

    Predicate<Document> docPredicate() {
        return docPredicate;
    }
}
