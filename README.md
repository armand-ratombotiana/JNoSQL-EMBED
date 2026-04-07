# JNoSQL-EMBED

> The embedded multi-model NoSQL database for the JVM.

JNoSQL-EMBED is a lightweight embedded NoSQL database written entirely in Java that implements the Jakarta NoSQL specification.

Think of it as **H2 for the NoSQL world** — a fast, tiny database you can embed directly into JVM applications.

## Features

- **Multi-model**: Document, Key-Value stores
- **ACID Transactions**: Commit/rollback with write-ahead logging
- **Tiny footprint**: <5MB JAR, minimal dependencies
- **Embedded mode**: Zero-config, no server required
- **Pluggable storage engines**: In-Memory, File-based persistence
- **Rich query API**: Equality, range, regex, AND/OR composition
- **TTL support**: Auto-expiring key-value entries
- **Docker ready**: Multi-stage build, minimal image

## Quick Start

### Maven

```xml
<dependency>
    <groupId>org.jnosql.embed</groupId>
    <artifactId>jnosql-embed-core</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

```kotlin
implementation("org.jnosql.embed:jnosql-embed-core:1.0.0")
```

## Usage

### Document Store

```java
JNoSQL db = JNoSQL.embed().build();

DocumentCollection users = db.documentCollection("users");

// Insert
Document user = users.insert(
    Document.of("name", "Alice").add("age", 30).add("email", "alice@example.com")
);

// Find by ID
Document found = users.findById(user.id());

// Query
List<Document> adults = users.find(Query.gt("age", 18));
List<Document> alice = users.find(Query.eq("name", "Alice"));

// Update
user.add("age", 31);
users.update(user);

// Delete
users.deleteById(user.id());
```

### Key-Value Store

```java
KeyValueBucket cache = db.keyValueBucket("cache");

// Basic operations
cache.put("session:123", "user-data");
String value = cache.get("session:123");

// With TTL
cache.put("token:abc", "secret", Duration.ofMinutes(30));

// Atomic counters
cache.increment("page-views");
cache.increment("page-views", 5);
```

### Transactions

```java
try (Transaction tx = db.beginTransaction()) {
    DocumentCollection accounts = tx.documentCollection("accounts");
    accounts.insert(Document.of("user", "Alice").add("balance", 1000));
    tx.commit();
}
// Auto-rollback if exception occurs or tx.close() without commit
```

### File Persistence

```java
JNoSQL db = JNoSQL.embed()
    .storageEngine(StorageEngineType.FILE)
    .persistTo("./data")
    .build();
// Data persists across restarts
```

## Docker

```bash
# Build
docker build -t jnosql-embed .

# Run with docker-compose
docker-compose up -d

# Run standalone with data volume
docker run -d -v jnosql-data:/data jnosql-embed
```

## Build from Source

```bash
mvn clean install
mvn test
```

## Architecture

```
JNoSQL-EMBED
├── Storage Engine (pluggable)
│   ├── InMemoryEngine — ConcurrentHashMap-backed, fastest
│   └── FileEngine — JSON file persistence, durable
├── Document Model
│   ├── Document — JSON-like with nested fields
│   ├── DocumentCollection — CRUD + query API
│   └── Query — Equality, range, regex, AND/OR
├── Key-Value Model
│   ├── KeyValueBucket — Get/Put/Delete
│   └── TTL support — Auto-expiration
└── Transactions
    ├── Commit/Rollback
    └── Auto-close with rollback
```

## Roadmap

| Version | Focus | Features |
|---------|-------|----------|
| **1.0** | Core engine | Document, KV, transactions, file persistence |
| **1.1** | Developer experience | Query language, B-Tree/LSM engines, Spring Boot starter |
| **1.2** | Framework integrations | Quarkus, Micronaut |
| **1.3** | AI features | Vector search (HNSW), LangChain4j |
| **2.0** | Distributed | Replication, clustering, sharding |

## License

MIT — see [LICENSE](LICENSE)
