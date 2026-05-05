# Changelog

All notable changes to JunifyDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-01-15

### Added
- **Hybrid SQL + NoSQL Engine**: Execute SQL queries and NoSQL operations on the same data
- **H2 Storage Engine**: Full SQL support via embedded H2 database
- **Enhanced Web Console**: Modern UI with hybrid SQL+NoSQL editor
- **Real-time Metrics Dashboard**: Monitor inserts, reads, updates, deletes, queries, transactions
- **Vector Search**: HNSW index for AI/ML embeddings with cosine similarity
- **Multiple Storage Engines**: In-Memory, File-based, LSM-Tree, B-Tree, H2
- **Query Explain Plans**: Performance optimization insights
- **Schema Browser**: Visual collection management
- **Backup/Restore API**: Web-based backup and restore operations
- **Activity Log**: Real-time monitoring of all database operations
- **Transaction Badges**: Visual indicators for active transactions
- **Framework Integrations**: Spring Boot, Quarkus, Micronaut support
- **Docker Support**: Multi-stage builds with health checks
- **CI/CD Pipeline**: GitHub Actions with Java 21 testing

### Changed
- **Branding**: Renamed from JNoSQL-EMBED to JunifyDB
- **Package Structure**: Changed from `org.junify.db.*` to `org.junify.db.*`
- **Maven Coordinates**: Now `org.junify.db:junifydb-core:1.0.0`
- **Java Version**: Downgraded from Java 25 preview to Java 21 LTS
- **Main Class**: Changed to `org.junify.db.console.JunifyConsole`
- **Web Console Version**: Updated to v1.0.0 with enhanced UI/UX
- **Docker Images**: Tagged as `junifydb:1.0.0` and `junifydb:latest`
- **Volume Names**: Aligned to `junifydb-data` for consistency

### Removed
- **Java Preview Features**: No longer requires `--enable-preview` flag
- **JNoSQL Branding**: Fully migrated to JunifyDB identity

### Fixed
- **Docker Volume Naming**: Corrected mismatched volume names in docker-compose.yml
- **Maven Artifact Naming**: Aligned all artifact names to junifydb-core
- **CI Workflow**: Updated to use Java 21 and correct image tags

### Security
- **AES-256-GCM Encryption**: Built-in encryption service for data at rest
- **Health Check Endpoints**: Secure monitoring via `/api/health`

### Performance
- **In-Memory Engine**: 1M+ operations per second
- **File Engine (async)**: 100K+ operations per second
- **H2 SQL Engine**: 50K+ queries per second
- **Vector Search**: Sub-millisecond similarity search for 100K vectors

### Documentation
- **README.md**: Complete rewrite with JunifyDB branding and hybrid SQL+NoSQL examples
- **RELEASE_NOTES.md**: Comprehensive v1.0.0 feature documentation
- **CONTRIBUTING.md**: Updated contribution guidelines
- **Docker Documentation**: Enhanced with web console access instructions

## [1.0.0] - 2023-12-01 (Beta)

### Added
- Initial beta release as JNoSQL-EMBED
- Document store with CRUD operations
- Key-Value store with TTL support
- In-Memory and File-based storage engines
- Basic transaction support
- Event system for operation hooks
- Metrics and monitoring
- HTTP REST API server
- Basic web console

---

## Upgrade Guide

### From 1.0.0 to 1.0.0

1. **Update Dependencies**
   ```xml
   <!-- Old -->
   <dependency>
       <groupId>org.junify.db</groupId>
       <artifactId>jnosql-embed-core</artifactId>
       <version>1.0.0</version>
   </dependency>
   
   <!-- New -->
   <dependency>
       <groupId>org.junify.db</groupId>
       <artifactId>junifydb-core</artifactId>
       <version>1.0.0</version>
   </dependency>
   ```

2. **Update Imports**
   ```java
   // Old
   import org.junify.db.JNoSQL;
   import org.junify.db.document.DocumentCollection;
   
   // New
   import org.junify.db.JunifyDB;
   import org.junify.db.document.DocumentCollection;
   ```

3. **Update Configuration**
   ```java
   // Old
   JNoSQL db = JNoSQL.embed().build();
   
   // New (same API, just renamed)
   JunifyDB db = JunifyDB.embed().build();
   ```

4. **Update Docker**
   ```bash
   # Old
   docker run -d -p 8080:8080 jnosql-embed
   
   # New
   docker run -d -p 8080:8080 junifydb:1.0.0
   ```

5. **Upgrade Java**
   - Ensure Java 21 LTS is installed
   - Remove `--enable-preview` flags from build scripts

---

## Future Releases

### [1.1.0] - Planned Q2 2024
- Query optimizer with cost-based execution plans
- Advanced indexing strategies
- Query result caching layer
- Performance improvements

### [1.2.0] - Planned Q3 2024
- LangChain4j integration
- RAG pipeline support
- Semantic search capabilities
- GraphQL API endpoint

### [1.3.0] - Planned Q4 2024
- Encryption enhancements
- Audit logging
- Role-based access control
- Enterprise features

### [2.0.0] - Planned 2025
- Distributed mode with replication
- Clustering support
- Sharding capabilities
- Distributed transactions

---

[1.0.0]: https://github.com/junifydb/junifydb/releases/tag/v1.0.0
[1.0.0]: https://github.com/junifydb/junifydb/releases/tag/v1.0.0
