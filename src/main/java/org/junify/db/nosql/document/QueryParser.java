package org.junify.db.nosql.document;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * MongoDB-style Query Parser for JunifyDB.
 * 
 * Parses JSON queries like:
 * {"age": {"$gt": 18, "$lt": 65}, "name": {"$regex": "^A"}}
 * 
 * Supported operators:
 * - $eq, $ne, $gt, $gte, $lt, $lte
 * - $in, $nin
 * - $regex
 * - $exists
 * - $and, $or
 */
public class QueryParser {

    private QueryParser() {
        // Utility class
    }

    /**
     * Parse a MongoDB-style query map into a Query object.
     */
    public static Query parse(Map<String, Object> queryMap) {
        if (queryMap == null || queryMap.isEmpty()) {
            return Query.all();
        }

        Predicate<Document> combined = doc -> true;

        for (Map.Entry<String, Object> entry : queryMap.entrySet()) {
            String field = entry.getKey();
            Object criteria = entry.getValue();

            Predicate<Document> fieldPredicate = parseField(field, criteria);
            combined = combined.and(fieldPredicate);
        }

        return new Query(combined, null, null, Integer.MAX_VALUE, 0);
    }

    @SuppressWarnings("unchecked")
    private static Predicate<Document> parseField(String field, Object criteria) {
        if (criteria instanceof Map) {
            return parseOperators(field, (Map<String, Object>) criteria);
        } else {
            // Simple equality
            return Query.eq(field, criteria).docPredicate();
        }
    }

    @SuppressWarnings("unchecked")
    private static Predicate<Document> parseOperators(String field, Map<String, Object> operators) {
        Predicate<Document> combined = doc -> true;

        for (Map.Entry<String, Object> op : operators.entrySet()) {
            String operator = op.getKey();
            Object value = op.getValue();

            Predicate<Document> opPredicate = switch (operator) {
                case "$eq" -> parseEq(field, value);
                case "$ne" -> parseNe(field, value);
                case "$gt" -> parseGt(field, value);
                case "$gte" -> parseGte(field, value);
                case "$lt" -> parseLt(field, value);
                case "$lte" -> parseLte(field, value);
                case "$in" -> parseIn(field, (List<Object>) value);
                case "$nin" -> parseNin(field, (List<Object>) value);
                case "$regex" -> parseRegex(field, value.toString());
                case "$exists" -> parseExists(field, (Boolean) value);
                case "$and" -> parseAnd((List<Map<String, Object>>) value);
                case "$or" -> parseOr((List<Map<String, Object>>) value);
                default -> doc -> true; // Unknown operator - ignore
            };

            combined = combined.and(opPredicate);
        }

        return combined;
    }

    private static Predicate<Document> parseEq(String field, Object value) {
        return doc -> {
            if (!doc.has(field)) return value == null;
            var v = doc.getRaw(field);
            return v != null ? v.equals(value) : value == null;
        };
    }

    private static Predicate<Document> parseNe(String field, Object value) {
        return parseEq(field, value).negate();
    }

    private static Predicate<Document> parseGt(String field, Object value) {
        return doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            if (v instanceof Number n && value instanceof Number nv) {
                return n.doubleValue() > nv.doubleValue();
            }
            return false;
        };
    }

    private static Predicate<Document> parseGte(String field, Object value) {
        return doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            if (v instanceof Number n && value instanceof Number nv) {
                return n.doubleValue() >= nv.doubleValue();
            }
            return false;
        };
    }

    private static Predicate<Document> parseLt(String field, Object value) {
        return doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            if (v instanceof Number n && value instanceof Number nv) {
                return n.doubleValue() < nv.doubleValue();
            }
            return false;
        };
    }

    private static Predicate<Document> parseLte(String field, Object value) {
        return doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            if (v instanceof Number n && value instanceof Number nv) {
                return n.doubleValue() <= nv.doubleValue();
            }
            return false;
        };
    }

    private static Predicate<Document> parseIn(String field, List<Object> values) {
        return doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            return values.contains(v);
        };
    }

    private static Predicate<Document> parseNin(String field, List<Object> values) {
        return parseIn(field, values).negate();
    }

    private static Predicate<Document> parseRegex(String field, String pattern) {
        return doc -> {
            if (!doc.has(field)) return false;
            var v = doc.getRaw(field);
            if (v == null) return false;
            return v.toString().matches(pattern);
        };
    }

    private static Predicate<Document> parseExists(String field, Boolean shouldExist) {
        return doc -> doc.has(field) == shouldExist;
    }

    private static Predicate<Document> parseAnd(List<Map<String, Object>> queries) {
        Predicate<Document> combined = doc -> true;
        for (Map<String, Object> q : queries) {
            combined = combined.and(parse(q).docPredicate());
        }
        return combined;
    }

    private static Predicate<Document> parseOr(List<Map<String, Object>> queries) {
        Predicate<Document> combined = doc -> false;
        for (Map<String, Object> q : queries) {
            combined = combined.or(parse(q).docPredicate());
        }
        return combined;
    }
}
