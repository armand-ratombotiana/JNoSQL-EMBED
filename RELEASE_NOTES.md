# JNoSQL-EMBED v1.0.0 Release Notes

## Overview
JNoSQL-EMBED v1.0.0 is the first stable release of the embedded multi-model NoSQL database for the JVM. This release includes a comprehensive feature set suitable for production use in embedded and edge computing scenarios.

## Features

### Core Functionality
- **Multi-model Storage**: Document, Key-Value, Column Family (Cassandra-style)
- **ACID Transactions**: Commit/rollback with automatic rollback on close
- **Query System**: Equality, range, regex, IN, EXISTS, AND/OR composition
- **Sorting & Pagination**: ASC/DESC sort, limit, offset, pagination
- **Storage Engines**: In-Memory (fastest) and File-based (JSON persistence)
- **TTL Support**: Auto-expiring key-value entries
- **Atomic Counters**: Increment/decrement operations

### Advanced Features
- **Event System**: Pre/post hooks for all operations (insert, update, delete, commit, rollback)
- **Metrics & Monitoring**: Real-time operation counters, ops/sec calculation
- **Backup/Restore**: GZIP-compressed backup and restore
- **HTTP Server**: REST API server with health endpoint
- **Encryption at Rest**: AES-256-GCM encryption service
- **Vector Search**: Cosine and Euclidean similarity search (HNSW-ready)
- **CLI Tool**: Interactive shell for database administration

### Framework Integrations
- **Spring Boot Starter**: Auto-configuration with properties
- **Quarkus Extension**: Build-time and runtime modules
- **Jakarta NoSQL Compatible**: Specification-aligned APIs

### DevOps & Tooling
- **Docker Support**: Multi-stage build with healthcheck
- **CI/CD Pipeline**: GitHub Actions with multi-Java testing, coverage, benchmarks
- **Benchmarking**: JMH benchmark suite included
- **Build Tool**: Maven 4.0+ with shade plugin for fat JAR
- **Code Quality**: JaCoCo enforcement (70% minimum coverage)

## Getting Started

### Maven Dependency
```xml
<dependency>
    <groupId>org.jnosql.embed</groupId>
    <artifactId>jnosql-embed-core</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Basic Usage
```java
JNoSQL db = JNoSQL.embed()
    .storageEngine(JNoSQLConfig.StorageEngineType.FILE)
    .persistTo("./data")
    .build();

DocumentCollection users = db.documentCollection("users");
Document alice = users.insert(
    Document.of("name", "Alice")
        .add("email", "alice@example.com")
        .add("age", 30)
);

// Query examples
var adults = users.find(Query.gte("age", 18));
var aliceAgain = users.findOne(Query.eq("name", "Alice"));
```

### Spring Boot
```yaml
# application.yml
jnosql:
  storage-engine: FILE
  data-dir: ./data
  auto-flush: true
  flush-interval-ms: 1000
```

```java
@Service
public class UserService {
    @Autowired
    private JNoSQL db;
    
    public User save(User user) {
        DocumentCollection users = db.documentCollection("users");
        return users.insert(user.toDocument());
    }
}
```

### Docker
```bash
docker build -t jnosql-embed .
docker run -d -p 8080:8080 -v jnosql-data:/data jnosql-embed
```

## Release Artifacts
- **Fat JAR**: `target/jnosql-embed-core-1.0.0.jar` (6.6MB with dependencies)
- **Original JAR**: `target/original-jnosql-embed-core-1.0.0.jar` (28KB)
- **Docker Image**: `jnosql-embed:latest` (built via Dockerfile)
- **Spring Boot Starter**: `spring-boot-starter/target/jnosql-embed-spring-boot-starter-1.0.0.jar`
- **Quarkus Extension**: `quarkus-extension/target/*`

## Test Results
- **Total Tests**: 79+ passing
- **Test Categories**: Unit, integration, concurrency, persistence, events, metrics, HTTP server
- **Coverage**: JaCoCo enforced minimum 70%
- **Benchmarks**: JMH benchmarks available via `mvn -Pbenchmark verify`

## Upcoming Features (v1.1+)
- SQL-like query language
- B-Tree and LSM-Tree storage engines
- Spring Data repository support
- GraphQL API endpoint
- Full-text search capabilities
- Replication and clustering (v2.0)

## License
MIT License - see LICENSE file

## Contributing
See CONTRIBUTING.md for guidelines on contributing to JNoSQL-EMBED.