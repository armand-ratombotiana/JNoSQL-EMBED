# JunifyDB

> The embedded multi-model NoSQL database for the JVM with SQL support.

JunifyDB (formerly JNoSQL-EMBED) is a lightweight embedded NoSQL database written entirely in Java that implements the Jakarta NoSQL specification. Think of it as **H2 for the NoSQL world** — a fast, tiny database you can embed directly into JVM applications.

## Features

### Multi-Model Support
- **Document Store**: JSON-like documents with rich query API
- **Key-Value Store**: Fast in-memory or persistent caching
- **Column-Family**: Wide-column store for sparse data
- **SQL (H2)**: Full relational queries with JOINs, views, triggers

### Storage Engines
- **In-Memory**: Fastest for ephemeral data and testing
- **File-based**: Persistent JSON storage with WAL
- **B-Tree**: Sorted indexes with range queries
- **LSM-Tree**: Optimized for writes with bloom filter
- **H2**: Full SQL with JDBC compatibility

### Advanced Features
- **ACID Transactions**: MVCC with savepoints
- **Full-Text Search**: TF-IDF ranking with highlighting
- **Vector Search**: HNSW for similarity search
- **CDC**: Change Data Capture for Kafka/File output
- **Replication**: Master-slave async replication
- **Query Cache**: LRU with TTL support

### Security
- **API Key Authentication**: Per-endpoint auth
- **Rate Limiting**: 1000 req/min per IP
- **CORS**: Cross-origin support
- **Compression**: GZIP response compression

### Framework Integration
- Spring Boot Starter
- Quarkus Extension

## Installation

### Maven

```xml
<dependency>
    <groupId>org.junify.db</groupId>
    <artifactId>junify-db-core</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

```kotlin
implementation("org.junify.db:junify-db-core:1.0.0")
```

## Quick Start

### Embedded Database

```java
var db = JunifyDB.embed()
    .storageEngine("IN_MEMORY") // or FILE, H2, B_TREE, LSM_TREE
    .persistTo("data")
    .build();
```

### Document Store

```java
var users = db.documentCollection("users");

// Insert
var user = new Document();
user.id("user-1");
user.add("name", "Alice");
user.add("email", "alice@example.com");
user.add("age", 30);
users.insert(user);

// Query
var results = users.find(
    Query.builder()
        .add("age", QueryCondition.GREATER_THAN, 25)
        .build()
);

// Stream
users.stream()
    .filter(d -> d.get("name").equals("Alice"))
    .forEach(System.out::println);
```

### Key-Value Store

```java
var cache = db.keyValueBucket("cache");

// Put
cache.put("user:1", "{\"name\": \"Alice\"}");
cache.put("user:2", "{\"name\": \"Bob\"}");

// Get
var value = cache.get("user:1");

// TTL
cache.put("session:abc", "data", 3600); // 1 hour TTL
```

### SQL (H2)

```java
var h2 = db.h2Engine();

// Create table
h2.executeSql("""
    CREATE TABLE users (
        id INT PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255)
    )
    """);

// Insert
h2.executeSql("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");

// Query
var result = h2.executeSql("SELECT * FROM users WHERE name = 'Alice'");
result.rows().forEach(row -> System.out.println(row));
```

### REST API

```bash
# Start server
java -jar junify-db-core.jar --port 8080 --engine FILE --data-dir ./data

# Health check
curl http://localhost:8080/api/health

# Create document
curl -X POST http://localhost:8080/api/collections/users \
    -H "Content-Type: application/json" \
    -d '{"name": "Alice", "email": "alice@example.com"}'

# Query
curl http://localhost:8080/api/collections/users

# SQL
curl -X POST http://localhost:8080/api/sql \
    -H "Content-Type: application/json" \
    -d 'SELECT * FROM users'
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `--port` | 8080 | HTTP server port |
| `--engine` | FILE | Storage engine (FILE, IN_MEMORY, B_TREE, LSM_TREE, H2) |
| `--data-dir` | data | Data directory |
| `--sync` | true | Synchronous flush |
| `--async` | false | Asynchronous flush |
| `--flush-interval` | 1000 | Flush interval (ms) |
| `--api-key` | - | API key for authentication |

## API Endpoints

| Endpoint | Method | Description |
|---------|--------|-------------|
| `/api/health` | GET | Health check with system metrics |
| `/api/collections/{name}` | GET/POST | Document CRUD |
| `/api/collections/{name}/{id}` | GET/PUT/DELETE | Single document |
| `/api/kv/{bucket}/{key}` | GET/PUT/DELETE | Key-Value operations |
| `/api/columns/{name}/{key}` | GET/PUT/DELETE | Column-family ops |
| `/api/bulk/{collection}` | POST | Batch operations |
| `/api/sql` | POST | SQL execution (H2) |
| `/api/cdc` | GET/POST | CDC management |
| `/api/schema` | GET | List tables |
| `/api/tables/{name}` | GET/POST/DELETE | Table management |

## Benchmarks

```bash
# Run benchmark
java -cp target/junify-db-core-1.0.0.jar org.junify.db.benchmark.BenchmarkRunner \
    --ops 10000 --engine IN_MEMORY --workload all
```

## Testing

```bash
# Run all tests
mvn test

# Run specific test
mvn test -Dtest=SchemaManagerTest
```

## Building

```bash
# Build JAR
mvn package -DskipTests

# Build with tests
mvn package
```

## License

Apache License 2.0