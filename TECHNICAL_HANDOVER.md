# JunifyDB Technical Handover Document
## For Qwen AI Model / Future Developers

---

## Table of Contents
1. [Executive Summary](#1-executive-summary)
2. [Project Architecture](#2-project-architecture)
3. [Core Components Deep Dive](#3-core-components-deep-dive)
4. [API Endpoints Reference](#4-api-endpoints-reference)
5. [Storage Engines](#5-storage-engines)
6. [Data Models](#6-data-models)
7. [Web Console](#7-web-console)
8. [Known Issues and Limitations](#8-known-issues-and-limitations)
9. [Development Guidelines](#9-development-guidelines)
10. [Testing Procedures](#10-testing-procedures)
11. [Deployment Guide](#11-deployment-guide)
12. [Troubleshooting](#12-troubleshooting)
13. [Best Practices](#13-best-practices)
14. [Future Work](#14-future-work)

---

## 1. Executive Summary

### What is JunifyDB?
JunifyDB is an embedded multi-model NoSQL database for the JVM that provides:
- **Document Storage**: JSON-like document collections
- **Key-Value Store**: Bucket/key-based storage
- **Column-Family**: Wide-column storage similar to Cassandra
- **Vector Search**: HNSW-based similarity search
- **SQL Interface**: Full SQL querying via H2 engine
- **ACID Transactions**: MVCC-based transaction support
- **CDC**: Change Data Capture for event streaming

### Technology Stack
- **Language**: Java 17+ (tested with Java 24)
- **Build Tool**: Maven 3.9+
- **SQL Backend**: H2 Database 2.4.240
- **JSON Processing**: Jackson 2.17.0
- **HTTP Server**: Java HttpServer (built-in)

### Current Status
- **Main Code**: Compiles successfully ✅
- **Tests**: Some broken (API mismatch) ⚠️
- **Web Console**: Fully functional ✅
- **API**: Core endpoints working ⚡

---

## 2. Project Architecture

### Directory Structure
```
JNoSQL-EMBED/
├── src/main/java/org/junify/db/
│   ├── JunifyDB.java              # Main entry point
│   ├── JunifyDBConfig.java       # Configuration class
│   │
│   ├── config/                    # Configuration beans
│   │
│   ├── console/http/             # HTTP Server
│   │   └── JunifyDBServer.java   # All REST endpoints (~900 lines)
│   │
│   ├── storage/spi/              # Storage layer
│   │   ├── H2StorageEngine.java  # SQL backend (most mature)
│   │   ├── InMemoryEngine.java   # In-memory storage
│   │   ├── FileEngine.java       # File-based persistence
│   │   ├── BTreeEngine.java      # B-tree implementation
│   │   ├── LSMTreeEngine.java    # LSM tree implementation
│   │   ├── SchemaManager.java    # Schema operations
│   │   ├── QueryOptimizer.java   # Query optimization
│   │   ├── DatabaseMetaManager.java
│   │   ├── FullTextSearchManager.java
│   │   ├── ReplicationManager.java
│   │   ├── SequenceManager.java
│   │   ├── StoredProcedureManager.java
│   │   ├── TriggerManager.java
│   │   ├── UserManager.java
│   │   ├── ViewManager.java
│   │   ├── ConstraintManager.java
│   │   ├── AnalyticFunctionManager.java
│   │   ├── CTEAndRecursiveManager.java
│   │   ├── WindowFunctionManager.java
│   │   └── StorageEngine.java    # Interface
│   │
│   ├── nosql/                    # NoSQL APIs
│   │   ├── document/
│   │   │   ├── Document.java
│   │   │   ├── DocumentCollection.java
│   │   │   ├── Query.java
│   │   │   ├── TextSearch.java
│   │   │   └── AggregationPipeline.java
│   │   ├── kv/
│   │   │   └── KeyValueBucket.java
│   │   └── column/
│   │       └── ColumnFamily.java
│   │
│   ├── index/                    # Index implementations
│   │   ├── hnsw/
│   │   │   ├── HNSWIndex.java   # Vector search
│   │   │   └── VectorIndex.java
│   │   ├── TextIndex.java
│   │   └── SecondaryIndex.java
│   │
│   ├── transaction/mvcc/          # Transaction support
│   │   ├── Transaction.java
│   │   └── MVCCManager.java
│   │
│   └── core/                      # Utilities
│       ├── cdc/                   # Change Data Capture
│       ├── cache/                 # Query caching
│       ├── backup/                # Backup/Restore
│       ├── metrics/               # Metrics collection
│       └── health/                # Health checks
│
├── src/main/resources/
│   └── static/
│       └── index.html             # Web Console (~2200 lines)
│
├── src/test/                      # Tests (some broken)
│
├── test-project/                  # Integration test projects
│   ├── simple-java/
│   ├── spring-boot/
│   ├── quarkus/
│   └── micronaut/
│
├── micronaut-integration/         # Micronaut module
├── spring-boot-starter/          # Spring Boot module
├── quarkus-extension/            # Quarkus module
│
└── pom.xml                        # Maven configuration
```

### Component Relationships

```
┌─────────────────────────────────────────────────────────────┐
│                      JunifyDB.main()                         │
│                    (Entry Point)                            │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    JunifyDB.create()                        │
│                  (Factory Method)                           │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                 JunifyDBConfig.StorageEngineType             │
│    IN_MEMORY | FILE | H2 | LSM_TREE | B_TREE                │
└─────────────────────────────────────────────────────────────┘
                            │
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
    ┌───────────┐   ┌───────────┐   ┌───────────┐
    │InMemory   │   │H2Storage  │   │FileEngine │
    │Engine     │   │Engine     │   │           │
    └───────────┘   └───────────┘   └───────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    H2StorageEngine                           │
│  - Manages SQL connections                                 │
│  - Schema operations via SchemaManager                     │
│  - Query execution via SqlHandler                           │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   JunifyDBServer                            │
│  - HTTP endpoints (18 contexts)                             │
│  - Health, Metrics, Stats, SQL, Collections, KV, etc.       │
└─────────────────────────────────────────────────────────────┘
                            │
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
    ┌───────────┐   ┌───────────┐   ┌───────────┐
    │Document   │   │KeyValue   │   │Column     │
    │API        │   │API        │   │Family API │
    └───────────┘   └───────────┘   └───────────┘
```

---

## 3. Core Components Deep Dive

### 3.1 JunifyDB.java (Main Entry)

**Location**: `src/main/java/org/junify/db/JunifyDB.java`

**Key Methods**:
```java
// Create database instance
public static JunifyDB create(JunifyDBConfig config)

// Builder pattern for configuration
public static JunifyDB embed()
    .storageEngine(StorageEngineType.H2)
    .persistTo("data")
    .autoFlush(true)
    .flushIntervalMs(1000)
    .buildConfig()

// Start HTTP server
public JunifyDBServer startServer(int port)

// Close database
public void close()
```

**Usage**:
```java
var config = JunifyDB.embed()
    .storageEngine(JunifyDBConfig.StorageEngineType.H2)
    .buildConfig();

try (var db = JunifyDB.create(config)) {
    var server = db.startServer(8080);
    // Server running...
}
```

### 3.2 JunifyDBServer.java (HTTP Server)

**Location**: `src/main/java/org/junify/db/console/http/JunifyDBServer.java`

**Purpose**: Provides REST API for all database operations

**Important Note**: 
- The HTTP server uses Java's built-in HttpServer
- Context paths cannot have prefix conflicts (e.g., `/api/` conflicts with `/api/collections/`)
- Always use trailing slash for nested paths: `/api/collections/` NOT `/api/collections`

**Current Endpoints** (18 total):
```java
server.createContext("/", new StaticHandler());           // Web console
server.createContext("/api/collections/", new CollectionsHandler());
server.createContext("/api/kv/", new KeyValueHandler());
server.createContext("/api/columns/", new ColumnHandler());
server.createContext("/api/health", new HealthHandler());
server.createContext("/api/metrics", new MetricsHandler());
server.createContext("/api/stats", new StatsHandler());
server.createContext("/api/backup", new BackupHandler());
server.createContext("/api/indexes/", new IndexHandler());
server.createContext("/api/transactions", new TransactionHandler());
server.createContext("/api/schema/", new SchemaHandler());
server.createContext("/api/vectors/", new VectorHandler());
server.createContext("/api/sql", new SqlHandler());
server.createContext("/api/bulk", new BulkHandler());
server.createContext("/api/cdc", new CDCHandler());
server.createContext("/api/tables/", new TablesHandler());
server.createContext("/api/constraints/", new ConstraintsHandler());
server.createContext("/api/cors", new CorsPreflightHandler());
```

### 3.3 SchemaManager.java

**Location**: `src/main/java/org/junify/db/storage/spi/SchemaManager.java`

**Purpose**: Manages database schema operations

**Key Methods**:
```java
// Table operations
public boolean tableExists(String tableName)
public List<String> getTables()
public List<String> getColumns(String table)
public String getColumnType(String table, String column)
public Integer getColumnSize(String table, String column)
public Map<String, Object> getTableInfo(String tableName)

// DDL operations
public CreateResult createTable(String tableName, Map<String, Object> columns)
public DropResult dropTable(String tableName)
public DropResult dropTable(String tableName, boolean force)
```

**Important**: The CreateResult and DropResult are local record classes, NOT H2StorageEngine.SqlResult

### 3.4 H2StorageEngine.java

**Location**: `src/main/java/org/junify/db/storage/spi/H2StorageEngine.java`

**Purpose**: Primary storage engine with SQL support

**Key Methods**:
```java
public SqlResult executeSql(String sql)
public SchemaManager schemaManager()
public boolean isAutoCommit()
public int getTransactionIsolation()
public Connection connection()
```

**SqlResult Record** (canonical form):
```java
public record SqlResult(
    boolean success,
    List<String> columns,
    int affected,
    String message,
    List<Map<String, Object>> rows,
    List<String> allColumns
) {}
```

### 3.5 SqlResult Conflict Issue

**Problem**: Multiple manager classes originally had their own SqlResult records with different signatures, causing type conversion errors.

**Solution**: All managers now use:
```java
import static org.junify.db.storage.spi.H2StorageEngine.SqlResult;
```

**Managers Fixed**:
- DatabaseMetaManager
- FullTextSearchManager
- ReplicationManager
- SequenceManager
- StoredProcedureManager
- TriggerManager
- UserManager
- ViewManager

---

## 4. API Endpoints Reference

### 4.1 Health & Monitoring

#### GET /api/health
```json
// Response
{
  "status": "ok",
  "engine": "H2",
  "version": "1.0.0",
  "uptime": 70000,
  "open": true,
  "memory": {
    "max": 4244635648,
    "total": 268435456,
    "used": 50000000,
    "free": 218000000
  },
  "threads": {
    "active": 6,
    "daemon": 6
  }
}
```

#### GET /api/metrics
```json
// Response
{
  "uptimeMs": 70000,
  "totalOperations": 10,
  "opsPerSecond": 0.5,
  "inserts": 5,
  "updates": 2,
  "deletes": 1,
  "reads": 2,
  "queries": 0,
  "transactions": 1,
  "transactionCommits": 1,
  "transactionRollbacks": 0,
  "collections": {
    "products": 3,
    "users": 2
  }
}
```

#### GET /api/stats
```json
// Response
{
  "database": {
    "open": true,
    "engine": "H2"
  },
  "memory": {
    "maxMemory": 4244635648,
    "totalMemory": 268435456,
    "usedMemory": 50000000,
    "freeMemory": 218000000,
    "availableProcessors": 8
  },
  "threads": {
    "activeCount": 6
  }
}
```

### 4.2 SQL Operations (H2 Engine Only)

#### POST /api/sql
```bash
# Request
curl -X POST http://localhost:14000/api/sql \
  -H "Content-Type: text/plain" \
  -d "SELECT * FROM products WHERE price > 100"

# Response
{
  "type": "select",
  "columns": ["id", "name", "price", "category"],
  "rows": [
    {"id": 1, "name": "Laptop", "price": 1000, "category": "Electronics"},
    {"id": 3, "name": "Desk", "price": 300, "category": "Furniture"}
  ]
}
```

**Supported Operations**:
- SELECT with WHERE, ORDER BY, GROUP BY
- INSERT, UPDATE, DELETE
- CREATE TABLE, DROP TABLE
- Aggregations: COUNT, AVG, SUM, MIN, MAX
- JOINs (subqueries)
- CASE expressions

### 4.3 Table Operations

#### GET /api/tables/{name}
```bash
# Request
curl http://localhost:14000/api/tables/products

# Response
{"name": "products", "columns": []}
```

### 4.4 Backup Operations

#### GET /api/backup
```json
{
  "diskUsage": {
    "fileCount": 0,
    "dataDir": "data",
    "totalMB": "0,00 MB",
    "totalBytes": 0
  },
  "backup": {
    "description": "Use POST /api/backup to create backup",
    "restore": "Use POST /api/backup/restore with JSON body containing 'backupFile' path"
  },
  "collections": {}
}
```

### 4.5 CDC Operations

#### GET /api/cdc
```json
{
  "enabled": true,
  "eventsInLog": 0,
  "subscribers": 0,
  "kafkaConnectors": [],
  "fileConnectors": []
}
```

### 4.6 Index Operations

#### GET /api/indexes/{collection}
```bash
curl http://localhost:14000/api/indexes/products

# Response
{"collection": "products", "indexes": {}}
```

### 4.7 Transaction Operations

#### POST /api/transactions (Begin)
```bash
curl -X POST http://localhost:14000/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"isolation":"READ_COMMITTED"}'

# Response
{"status": "started", "transactionId": 413560608}
```

---

## 5. Storage Engines

### 5.1 IN_MEMORY Engine
- **Best for**: Testing, development, caching
- **Pros**: Fast, no file locks, no persistence
- **Cons**: Data lost on restart
- **Usage**: `--engine IN_MEMORY`

### 5.2 H2 Engine (Recommended for SQL)
- **Best for**: Applications needing SQL queries
- **Pros**: Full SQL support, mature, reliable
- **Cons**: File lock issues if not properly closed
- **Usage**: `--engine H2`
- **Note**: Always use `--engine IN_MEMORY` for testing to avoid file lock issues

### 5.3 File Engine
- **Best for**: Simple file-based persistence
- **Pros**: Simple, portable
- **Cons**: Limited features

### 5.4 BTree Engine
- **Best for**: Ordered data access
- **Pros**: Good for range queries
- **Cons**: Not fully implemented

### 5.5 LSMTree Engine
- **Best for**: Write-heavy workloads
- **Pros**: High write throughput
- **Cons**: Not fully implemented

---

## 6. Data Models

### 6.1 Document Model
```java
// API: /api/collections/{name}
// Operations: GET, POST, PUT, DELETE
// Status: Partially functional (POST may hang)
```

### 6.2 Key-Value Model
```java
// API: /api/kv/{bucket}/{key}
// Operations: GET, POST, DELETE
// Status: Not fully tested
```

### 6.3 Column-Family Model
```java
// API: /api/columns/{family}/{key}
// Operations: GET, PUT, DELETE
// Status: Not fully tested
```

### 6.4 Vector Model
```java
// API: /api/vectors/{index}/{id}
// Operations: GET, POST, DELETE, SEARCH
// Implementation: HNSW algorithm
// Status: Partial (POST may hang)
```

---

## 7. Web Console

### 7.1 Important: Dynamic API URL

The web console MUST use dynamic port detection:

```javascript
// CORRECT - Uses current browser port
const API = window.location.origin + '/api';

// WRONG - Hardcoded port
const API = 'http://localhost:8080/api';
```

**File Location**: `src/main/resources/static/index.html` (Line ~1386)

### 7.2 Features
- **Dashboard**: Health, metrics, stats monitoring
- **Documents Tab**: Create/manage collections
- **Query Tab**: Filter, sort, paginate documents
- **SQL Tab**: Full SQL console (H2 engine only)
- **Key-Value Tab**: Bucket/key operations
- **Column-Family Tab**: Column operations
- **Vectors Tab**: Vector similarity search
- **Transactions Tab**: Begin/commit/rollback
- **Indexes Tab**: Create and view indexes
- **Backup Tab**: Create/restore backups
- **Import/Export**: JSON data handling

### 7.3 Theme
- Dark theme (default)
- Light theme toggle in header
- Preference stored in localStorage

---

## 8. Known Issues and Limitations

### 8.1 High Priority Issues

| Issue | Description | Workaround |
|-------|-------------|------------|
| Document POST hangs | POST to /api/collections/{name} hangs server | Use SQL INSERT instead |
| Vector POST hangs | POST to /api/vectors/{index} hangs server | Not resolved |
| File lock (H2) | H2 database file locked | Use IN_MEMORY engine for testing |
| Metrics not tracking | totalOperations always 0 | Metrics collection not implemented |

### 8.2 Test Compilation Errors

Tests use old SchemaManager API that's no longer available:
- `ColumnDef.of()` method doesn't exist
- `createIndex()` method doesn't exist  
- `analyzeTable()` method doesn't exist

**Solution**: Either:
1. Update tests to use new API
2. Add missing methods back to SchemaManager
3. Delete broken tests

### 8.3 Missing Features

- Bulk operations endpoint (exists but not tested)
- Full transaction commit/rollback (partial)
- Reactive API stubs (not implemented)
- Adapter modules (removed due to dependency issues)

---

## 9. Development Guidelines

### 9.1 Code Style

**Naming Conventions**:
- Classes: PascalCase (e.g., `JunifyDB`, `H2StorageEngine`)
- Methods: camelCase (e.g., `getTables()`, `executeSql()`)
- Variables: camelCase (e.g., `tableName`, `sqlResult`)
- Constants: UPPER_SNAKE_CASE

**Record Classes** (for immutable data):
```java
public record SqlResult(
    boolean success,
    List<String> columns,
    int affected,
    String message,
    List<Map<String, Object>> rows,
    List<String> allColumns
) {}
```

### 9.2 Adding New API Endpoints

1. **Add handler class** in `JunifyDBServer.java`:
```java
private class NewFeatureHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        // Implementation
    }
}
```

2. **Register endpoint**:
```java
server.createContext("/api/newfeature", new NewFeatureHandler());
```

3. **Important**: Avoid path conflicts!
   - `/api/items` conflicts with `/api/items/123`
   - Use trailing slash: `/api/items/` for collections

### 9.3 Adding New Storage Engine

1. **Implement interface**:
```java
public class MyEngine implements StorageEngine {
    @Override
    public void initialize(String dataDir, boolean autoFlush, long flushInterval) {
        // Implementation
    }
    
    @Override
    public SqlResult executeSql(String sql) {
        // Implementation
    }
    
    // ... other methods
}
```

2. **Register in config**:
```java
// In JunifyDBConfig.StorageEngineType
case "MY_ENGINE" -> new MyEngine();
```

---

## 10. Testing Procedures

### 10.1 Unit Testing

**Compile main code only**:
```bash
mvn compile -DskipTests
```

### 10.2 Integration Testing

**Start server with IN_MEMORY**:
```bash
java -cp "target/classes;target/dep/*" org.junify.db.JunifyDB --port 14000 --engine IN_MEMORY
```

**Test endpoints**:
```powershell
# Health check
Invoke-WebRequest -Uri "http://localhost:14000/api/health"

# SQL test
Invoke-WebRequest -Uri "http://localhost:14000/api/sql" -Method POST -Body "SELECT 1" -ContentType "text/plain"
```

### 10.3 Web Console Testing

1. Start server: `java -cp ... org.junify.db.JunifyDB --port 14000 --engine IN_MEMORY`
2. Open: `http://localhost:14000`
3. Verify dashboard loads
4. Test SQL tab (won't work with IN_MEMORY, use H2)

---

## 11. Deployment Guide

### 11.1 Production Build

```bash
# Compile
mvn clean compile -DskipTests

# Package (creates JAR)
mvn package -DskipTests

# Copy dependencies
mvn dependency:copy-dependencies -DoutputDirectory=target/dep -DincludeScope=compile
```

### 11.2 Running in Production

```bash
# With H2 (SQL support)
java -Xmx2g -cp "junify-db-core.jar:dep/*" org.junify.db.JunifyDB \
  --port 8080 \
  --engine H2 \
  --data-dir /var/lib/junify \
  --sync

# With IN_MEMORY (fast, no SQL)
java -Xmx1g -cp "junify-db-core.jar:dep/*" org.junify.db.JunifyDB \
  --port 8080 \
  --engine IN_MEMORY
```

### 11.3 Docker (Future)

```dockerfile
FROM openjdk:21-slim
WORKDIR /app
COPY target/junify-db-core.jar app.jar
COPY target/dep/*.jar lib/
EXPOSE 8080
CMD ["java", "-cp", "app.jar:lib/*", "org.junify.db.JunifyDB", "--port", "8080", "--engine", "H2"]
```

---

## 12. Troubleshooting

### 12.1 "Database file is locked"

**Cause**: H2 database file still open from previous instance

**Solution**:
```bash
# Kill any Java processes using the database
taskkill /F /IM java.exe

# Or use IN_MEMORY engine for testing
--engine IN_MEMORY
```

### 12.2 "cannot add context to list"

**Cause**: Conflicting HTTP endpoint paths in JunifyDBServer.java

**Solution**: 
- Remove duplicate paths
- Use trailing slashes consistently: `/api/collections/` not `/api/collections`
- Avoid paths that are prefixes of other paths

### 12.3 Web console shows "Failed to fetch"

**Cause**: Wrong API URL or server not running

**Solution**:
1. Check server is running: `curl http://localhost:14000/api/health`
2. Verify API URL in index.html: `const API = window.location.origin + '/api'`
3. Check browser console for CORS errors

### 12.4 Test compilation errors

**Cause**: Test code uses old API

**Solution**:
```bash
# Skip tests during build
mvn compile -DskipTests
mvn package -DskipTests
```

---

## 13. Best Practices

### 13.1 Development

1. **Always use IN_MEMORY for development** - Avoids file lock issues
2. **Test SQL in H2, document ops in IN_MEMORY** - Each engine has different capabilities
3. **Check server logs** - Run with visible console output when debugging
4. **Use web console for manual testing** - Faster than curl for UI testing

### 13.2 Code Organization

1. **Keep handlers focused** - One handler per endpoint pattern
2. **Use records for data transfer** - Clean, immutable, thread-safe
3. **Import H2StorageEngine.SqlResult statically** - Avoids type conflicts

### 13.3 Error Handling

1. **Always return JSON errors** - Consistent API responses
2. **Log errors server-side** - Use appropriate log levels
3. **Handle exceptions in handlers** - Don't let exceptions bubble to HTTP server

### 13.4 Performance

1. **Use connection pooling** - H2StorageEngine manages this
2. **Batch operations** - Use bulk API when available
3. **Monitor metrics** - Watch /api/metrics for bottlenecks

---

## 14. Future Work

### 14.1 High Priority

1. **Fix hanging operations** - Document, Vector, KV POST operations
2. **Update tests** - Fix or remove broken test files
3. **Implement metrics tracking** - Currently always shows 0

### 14.2 Medium Priority

1. **Complete document CRUD** - Full collection operations
2. **Implement reactive API** - Currently stub only
3. **Add integration tests** - Test project integration

### 14.3 Low Priority

1. **Docker support** - Containerize application
2. **Cloud deployment** - Kubernetes manifests
3. **Monitoring integration** - Prometheus, Grafana
4. **Security** - Authentication, encryption

---

## Appendix A: Quick Reference Commands

```bash
# Build
mvn compile -DskipTests

# Copy deps
mvn dependency:copy-dependencies -DoutputDirectory=target/dep -DincludeScope=compile

# Start IN_MEMORY
java -cp "target/classes;target/dep/*" org.junify.db.JunifyDB --port 14000 --engine IN_MEMORY

# Start H2
java -cp "target/classes;target/dep/*" org.junify.db.JunifyDB --port 14000 --engine H2

# Test API
curl http://localhost:14000/api/health

# SQL test
curl -X POST http://localhost:14000/api/sql -H "Content-Type: text/plain" -d "SELECT 1"
```

---

## Appendix B: File Locations

| Component | File |
|-----------|------|
| Main | `JunifyDB.java` |
| Config | `JunifyDBConfig.java` |
| HTTP Server | `JunifyDBServer.java` |
| Web Console | `src/main/resources/static/index.html` |
| Schema Manager | `SchemaManager.java` |
| H2 Engine | `H2StorageEngine.java` |
| This Document | `HANDOVER.md` |

---

*Document Version: 1.0*
*Last Updated: May 2026*
*Branch: junify-retry*
*For Questions: Check HANDOVER.md or codebase comments*