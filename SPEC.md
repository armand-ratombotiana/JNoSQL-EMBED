# JNoSQL-EMBED v1.0 Specification

## Vision
The H2 of the NoSQL world — a lightweight, embeddable, multi-model NoSQL database for the JVM.

## Goals
- **Zero external dependencies** — single JAR, no server required
- **Tiny footprint** — <5MB JAR, <10MB RAM idle
- **Multi-model** — Document, Key-Value, Column Family in one engine
- **ACID compliant** — transactional guarantees with MVCC
- **Developer-first** — simple API, Jakarta NoSQL compatible
- **Production-ready** — durable storage, encryption, backup/restore

## Architecture

### Storage Layer
- **In-Memory Engine** — ConcurrentHashMap-backed, fastest, non-durable
- **LSM-Tree Engine** — Log-structured merge tree, write-optimized, durable
- **B-Tree Engine** — B+Tree index, read-optimized, durable

### Data Models

#### Document Model
- JSON-like documents with nested fields
- Collections with automatic indexing
- CRUD operations: insert, find, update, delete
- Query API: equality, range, regex, sorting, pagination

#### Key-Value Model
- String/binary keys with any serializable value
- TTL support for auto-expiration
- Atomic increment/decrement

#### Column Family Model
- Row-based column families (Cassandra-style)
- Sparse columns, per-row schema

### Transaction Engine
- ACID transactions with write-ahead log (WAL)
- MVCC (Multi-Version Concurrency Control)
- Isolation levels: Read Committed, Snapshot

### Vector Search (Phase 2)
- HNSW index for approximate nearest neighbor
- Cosine similarity, Euclidean distance

### Server Mode
- HTTP/REST API for remote access
- Built-in web console

## v1.0 Scope (1-Hour Ship)

### Must Have
- [x] Maven project structure with Java 21
- [x] Document store (CRUD + basic queries)
- [x] Key-value store (CRUD + TTL)
- [x] In-memory storage engine
- [x] File-based persistence (JSON serialization)
- [x] Basic transaction support (commit/rollback)
- [x] Unit tests
- [x] Docker support
- [x] Working examples

### Should Have (v1.1)
- [ ] LSM-Tree storage engine
- [ ] B-Tree storage engine
- [ ] Advanced query language
- [ ] Index optimization
- [ ] Spring Boot starter

### Nice to Have (v2.0)
- [ ] Vector search (HNSW)
- [ ] Column family model
- [ ] Server mode (HTTP/REST)
- [ ] Web console
- [ ] Encryption at rest

## API Design

```java
// Embed database
JNoSQL db = JNoSQL.embed()
    .storageEngine(StorageEngine.IN_MEMORY)
    .persistTo("data/")
    .build();

// Document API
DocumentCollection users = db.documentCollection("users");
users.insert(Document.of("name", "Alice").add("age", 30));
List<Document> results = users.find(Query.eq("name", "Alice"));

// Key-Value API
KeyValueBucket cache = db.keyValueBucket("cache");
cache.put("session:123", "user-data", Duration.ofHours(1));
String value = cache.get("session:123");

// Transactions
try (Transaction tx = db.beginTransaction()) {
    DocumentCollection col = tx.documentCollection("users");
    col.insert(Document.of("name", "Bob"));
    tx.commit();
} catch (Exception e) {
    tx.rollback();
}
```

## Project Structure
```
src/main/java/org/jnosql/embed/
├── JNoSQL.java                 # Main entry point
├── config/
│   ├── JNoSQLConfig.java       # Configuration builder
│   └── StorageEngine.java      # Storage engine enum
├── storage/
│   ├── StorageEngine.java      # Storage interface
│   ├── InMemoryEngine.java     # In-memory implementation
│   └── FileEngine.java         # File-based persistence
├── document/
│   ├── Document.java           # Document model
│   ├── DocumentCollection.java # Collection API
│   └── Query.java              # Query builder
├── kv/
│   ├── KeyValueBucket.java     # Key-value API
│   └── TTLManager.java         # TTL expiration
├── transaction/
│   ├── Transaction.java        # Transaction API
│   └── WAL.java                # Write-ahead log
└── util/
    └── JsonSerde.java          # JSON serialization

src/test/java/org/jnosql/embed/
├── DocumentCollectionTest.java
├── KeyValueBucketTest.java
└── TransactionTest.java
```

## Docker Strategy
- Multi-stage build for minimal image
- Alpine-based JRE runtime
- Expose port 8080 for future server mode
- Volume mount for data persistence

## Success Criteria
- All tests pass
- JAR < 5MB
- Docker image < 50MB
- Sub-millisecond in-memory operations
- Clean build with no warnings
