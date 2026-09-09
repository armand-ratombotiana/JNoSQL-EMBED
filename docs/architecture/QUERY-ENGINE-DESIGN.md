# JNOSQL-EMBED: Query Engine & Indexing Design

An architectural blueprint of the query parser, predicate execution engine, and secondary indexing mechanisms in JNOSQL-EMBED.

---

## 1. Query Abstraction (`Query.java`)

Queries are constructed using a type-safe fluent API or parsed from query strings:

```java
// Fluent Programmatic Query Construction
Query q1 = Query.eq("department", "Engineering");
Query q2 = Query.gt("age", 25);
Query q3 = Query.contains("bio", "Distributed systems");
Query q4 = Query.in("status", List.of("ACTIVE", "PENDING"));

// Query String Parsing
Query q5 = Query.fromQuery("department = Engineering");
```

### Supported Predicate Operators:
- `eq(field, value)`: Exact equality match.
- `ne(field, value)`: Inequality match.
- `gt(field, number)` / `gte(field, number)`: Greater than (or equal).
- `lt(field, number)` / `lte(field, number)`: Less than (or equal).
- `contains(field, substring)`: Substring match on string fields.
- `in(field, list)`: Membership test in value list.
- `all()`: Matches all records in the collection.

---

## 2. Query Execution & Optimization Strategy

```mermaid
graph TD
    A[Incoming Query] --> B{Does query target an indexed field via eq()?}
    B -- Yes --> C[Lookup Matching Doc IDs in SecondaryIndex]
    C --> D[Fetch Target Documents by ID from Storage Engine]
    D --> E[Apply Post-Filters, Limit, Offset, Sort]
    E --> F[Return Results List<Document>]
    
    B -- No --> G[Scan All Documents from Storage Engine]
    G --> H[Evaluate Java Predicate<Document> per Document]
    H --> E
```

### 2.1 Index-Assisted Fast Path
When `Query.eq(field, value)` is executed and a secondary index exists on `field`:
1. The query identifies `indexedField = field`.
2. `DocumentCollection` checks `indexes.get(indexedField)`.
3. If present, it executes `index.find(value)` which performs a sub-millisecond $O(1)$ set lookup returning the exact document IDs.
4. Only the matched document IDs are fetched from the underlying storage engine, avoiding a full table scan.

### 2.2 Full-Scan Fallback Path
When querying non-indexed fields or evaluating complex regex expressions:
1. `StorageEngine.scan(collection)` streams all documents in the collection.
2. The composite Java `Predicate<Document>` evaluates each document in memory.
3. Pagination (`offset`, `limit`) and sorting (`sortField`, `sortOrder`) are applied via Java stream operators.

---

## 3. Secondary Index Architecture (`SecondaryIndex.java`)

- **Structure**: An in-memory inverted index mapping field values to sets of document IDs:
  ```
  Map<Object, Set<String>> indexMap;
  ```
- **Lifecycle**:
  - `collection.createIndex("email")` immediately indexes all existing documents.
  - On subsequent `insert(doc)` or `update(doc)`, the index is incrementally updated.
  - On `delete(id)`, the document's entries are purged from the index.
- **Index Durability**:
  - Index definitions and mappings are saved to `.indexes` in the database directory on disk (`saveIndexes()`).
  - On database restart, `loadIndexes()` automatically rebuilds the secondary index states.
