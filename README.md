# JunifyDB

JunifyDB is an embedded NoSQL database for Java applications.

The goal is simple: provide the NoSQL equivalent of H2 for Java developers. Add a dependency, choose in-memory or local file storage, and use document/key-value data structures directly inside Spring Boot, Quarkus, Micronaut, tests, CLI tools, or desktop applications.

JunifyDB is designed around the Jakarta NoSQL / JNoSQL philosophy: Java-first APIs, embeddable runtime, simple persistence, and framework integration without requiring a separate database server.

## Project Focus

- Embedded database, not a required external service.
- JNoSQL-style document, key-value, and column-family APIs.
- In-memory mode for tests, demos, and temporary application state.
- File-backed mode for local persistence.
- Framework starters/extensions for Spring Boot, Quarkus, and Micronaut.
- Developer console for inspecting and editing embedded data during development.

Advanced features such as metrics, CDC, text search, vector search, and the REST console are useful, but the core product promise is the embedded Java developer experience.

## Installation

### Core

```xml
<dependency>
    <groupId>org.junify.db</groupId>
    <artifactId>junify-db-core</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Spring Boot Starter

```xml
<dependency>
    <groupId>org.junify.db</groupId>
    <artifactId>junify-db-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Quarkus Extension

```xml
<dependency>
    <groupId>org.junify.db</groupId>
    <artifactId>junify-db-quarkus-extension-runtime</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Micronaut Integration

```xml
<dependency>
    <groupId>org.junify.db</groupId>
    <artifactId>junifydb-micronaut-integration</artifactId>
    <version>1.0.0</version>
</dependency>
```

---

## Demonstration Ecosystem

A complete, production-grade demonstration suite implementing a canonical **E-Commerce & Order Management** domain is located in the [`demo/`](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo) directory:

- **[demo-common](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo/demo-common)**: Canonical domain models and test fixtures using Java records.
- **[spring-boot-demo](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo/spring-boot-demo)**: Spring Boot 3.2.0 REST application using `JunifyDBTemplate`.
- **[quarkus-demo](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo/quarkus-demo)**: Quarkus 3.8.0 reactive application using CDI producers.
- **[micronaut-demo](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo/micronaut-demo)**: Micronaut 4.2.0 application using reflection-free Serde.
- **[vertx-demo](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo/vertx-demo)**: Eclipse Vert.x 4.5.4 reactive verticle demonstrating thread-safe worker execution.
- **[end-to-end-validation](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo/end-to-end-validation)**: Multi-engine durability and cold restart matrix test suite.

## Quick Start

### Embedded In-Memory Database

```java
import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig.StorageEngineType;
import org.junify.db.nosql.document.Document;

try (var db = JunifyDB.create(JunifyDB.embed()
        .storageEngine(StorageEngineType.IN_MEMORY)
        .buildConfig())) {

    var users = db.documentCollection("users");

    var alice = new Document();
    alice.id("user-1");
    alice.add("name", "Alice");
    alice.add("email", "alice@example.com");
    alice.add("age", 30);

    users.insert(alice);

    var loaded = users.findById("user-1");
    System.out.println(loaded.get("name"));
}
```

### File-Backed Embedded Database

```java
try (var db = JunifyDB.create(JunifyDB.embed()
        .storageEngine(StorageEngineType.FILE)
        .persistTo("data")
        .autoFlush(true)
        .buildConfig())) {

    db.keyValueBucket("sessions").put("session-1", "active");
}
```

### Key-Value API

```java
var cache = db.keyValueBucket("cache");

cache.put("feature:signup", "enabled");
var value = cache.get("feature:signup");
cache.delete("feature:signup");
```

### Column-Family API

```java
var profiles = db.columnFamily("profiles");
profiles.put("user-1", "name", "Alice");
profiles.put("user-1", "country", "MG");
```

## Spring Boot

Add the starter and configure JunifyDB with normal Spring Boot properties:

```yaml
junifydb:
  enabled: true
  storage-engine: IN_MEMORY
  data-dir: data
  auto-flush: true
  flush-interval-ms: 1000
```

Inject either the database or the convenience template:

```java
import org.junify.db.nosql.document.Document;
import org.junify.db.spring.boot.JunifyDBTemplate;
import org.springframework.stereotype.Service;

@Service
class UserStore {
    private final JunifyDBTemplate db;

    UserStore(JunifyDBTemplate db) {
        this.db = db;
    }

    void saveUser(String id, String name) {
        var doc = new Document();
        doc.id(id);
        doc.add("name", name);
        db.documents("users").insert(doc);
    }
}
```

## Developer Console

JunifyDB can expose a local HTTP console for development and inspection:

```bash
java -jar target/junify-db-core-1.0.0.jar --port 8080 --engine FILE --data-dir ./data
```

Then open:

```text
http://localhost:8080
```

The console is intended as a developer convenience for viewing metrics, browsing collections, editing documents, and running simple queries.

## REST API

The embedded server exposes document and key-value endpoints when enabled:

```bash
curl http://localhost:8080/api/health

curl -X POST http://localhost:8080/api/collections/users \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"Alice\",\"email\":\"alice@example.com\"}"

curl http://localhost:8080/api/collections/users
```

## Storage Modes

| Mode | Use Case |
| --- | --- |
| `IN_MEMORY` | Tests, demos, short-lived app state |
| `FILE` | Simple local persistence |
| `B_TREE` | Sorted access patterns and range-oriented workloads |
| `LSM_TREE` | Write-heavy local workloads |

## Framework Roadmap

| Framework | Status | Goal |
| --- | --- | --- |
| Spring Boot | Starter aligned to embedded JunifyDB API | Add dependency, inject `JunifyDB` or `JunifyDBTemplate` |
| Quarkus | Extension sources present | CDI producer and build-time config |
| Micronaut | Integration sources present | Factory/repository beans with Micronaut configuration |

## Build And Test

```bash
mvn test
```

Build the Spring Boot starter after installing the core artifact locally:

```bash
mvn install -DskipTests
cd spring-boot-starter
mvn test
```

## License

Apache License 2.0
