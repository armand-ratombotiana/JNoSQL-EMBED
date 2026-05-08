# JunifyDB Quick Start Guide

## 🚀 Getting Started in 5 Minutes

### Prerequisites
- Java 17 or higher
- Maven 3.6+

### 1. Build the Project
```bash
mvn clean package -DskipTests
```

### 2. Start the Server
```bash
java -jar target/junify-db-core-1.0.0.jar --port 8080 --engine FILE --data-dir ./data
```

### 3. Test the API
```bash
# Health Check (use the API key shown in server output)
curl -H "X-API-Key: YOUR_API_KEY" http://localhost:8080/api/health
```

## 📚 Common Operations

### Document Store

#### Insert a Document
```bash
curl -X POST http://localhost:8080/api/collections/users \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"Alice","email":"alice@example.com","age":30}'
```

#### Get All Documents
```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  http://localhost:8080/api/collections/users
```

#### Get Document by ID
```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  http://localhost:8080/api/collections/users/{id}
```

#### Update Document
```bash
curl -X PUT http://localhost:8080/api/collections/users/{id} \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"Alice Updated","email":"alice@example.com","age":31}'
```

#### Delete Document
```bash
curl -X DELETE http://localhost:8080/api/collections/users/{id} \
  -H "X-API-Key: YOUR_API_KEY"
```

### Key-Value Store

#### Set a Value
```bash
curl -X PUT http://localhost:8080/api/kv/cache/session:123 \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"value":"session_data_here"}'
```

#### Get a Value
```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  http://localhost:8080/api/kv/cache/session:123
```

### List Operations

#### Push to List
```bash
curl -X POST http://localhost:8080/api/kv/lists/tasks/queue1/rpush \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"values":["task1","task2","task3"]}'
```

#### Get List
```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  http://localhost:8080/api/kv/lists/tasks/queue1
```

#### Pop from List
```bash
curl -X POST http://localhost:8080/api/kv/lists/tasks/queue1/lpop \
  -H "X-API-Key: YOUR_API_KEY"
```

### Set Operations

#### Add to Set
```bash
curl -X POST http://localhost:8080/api/kv/sets/tags/article1/sadd \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"members":["java","database","nosql"]}'
```

#### Get Set Members
```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  http://localhost:8080/api/kv/sets/tags/article1
```

### Hash Operations

#### Set Hash Fields
```bash
curl -X POST http://localhost:8080/api/kv/hashes/profiles/user:1/hset \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"fields":{"name":"Bob","email":"bob@example.com","age":"35"}}'
```

#### Get Hash
```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  http://localhost:8080/api/kv/hashes/profiles/user:1
```

## 💻 Embedded Usage

### Java Code Example

```java
import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.Document;

public class Example {
    public static void main(String[] args) {
        // Create embedded database
        var db = JunifyDB.embed()
            .storageEngine("FILE")
            .persistTo("data")
            .build();
        
        // Document operations
        var users = db.documentCollection("users");
        
        var user = new Document();
        user.id("user-1");
        user.add("name", "Alice");
        user.add("email", "alice@example.com");
        user.add("age", 30);
        
        users.insert(user);
        
        // Query
        var results = users.findAll();
        results.forEach(doc -> 
            System.out.println(doc.get("name"))
        );
        
        // Key-Value operations
        var cache = db.keyValueBucket("cache");
        cache.put("key1", "value1");
        String value = cache.get("key1");
        
        // Cleanup
        db.close();
    }
}
```

## 🔧 Configuration

### Storage Engines
- `IN_MEMORY` - Fastest, no persistence
- `FILE` - JSON file storage with WAL
- `B_TREE` - Sorted indexes, range queries
- `LSM_TREE` - Write-optimized with bloom filter
- `H2` - Full SQL support

### Server Options
```bash
java -jar junify-db-core-1.0.0.jar \
  --port 8080 \
  --engine FILE \
  --data-dir ./data \
  --sync \
  --flush-interval 1000 \
  --api-key YOUR_SECURE_KEY
```

## 📊 Monitoring

### Health Check
```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  http://localhost:8080/api/health
```

### Metrics
```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  http://localhost:8080/api/metrics
```

### Stats
```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  http://localhost:8080/api/stats
```

## 🔐 Security

### Change Default API Key
```bash
java -jar junify-db-core-1.0.0.jar --api-key YOUR_SECURE_KEY
```

### Disable Authentication (Development Only)
Edit `JunifyDBServer.java` and call:
```java
server.disableAuthentication();
```

### Enable SSL/TLS
```bash
java -jar junify-db-core-1.0.0.jar \
  --ssl-port 8443 \
  --ssl-keystore /path/to/keystore.jks \
  --ssl-keypass YOUR_PASSWORD
```

## 🧪 Testing

### Run All Tests
```bash
mvn test
```

### Run Specific Test
```bash
mvn test -Dtest=DocumentCollectionTest
```

### Run API Test Script
```bash
# PowerShell
.\test-api.ps1

# Bash
./test-api.sh
```

## 📦 Framework Integration

### Spring Boot
Add dependency:
```xml
<dependency>
    <groupId>org.junify.db</groupId>
    <artifactId>junify-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

Configure in `application.yml`:
```yaml
junify:
  storage-engine: FILE
  data-dir: ./data
  auto-flush: true
```

### Quarkus
Add extension:
```xml
<dependency>
    <groupId>org.junify.db</groupId>
    <artifactId>junify-quarkus-extension</artifactId>
    <version>1.0.0</version>
</dependency>
```

## 🐛 Troubleshooting

### Port Already in Use
```bash
# Use different port
java -jar junify-db-core-1.0.0.jar --port 8081
```

### Out of Memory
```bash
# Increase heap size
java -Xmx2g -jar junify-db-core-1.0.0.jar
```

### Data Corruption
```bash
# Use backup manager
curl -X POST http://localhost:8080/api/backup \
  -H "X-API-Key: YOUR_API_KEY"
```

## 📖 Additional Resources

- **Full Documentation:** See `README.md`
- **API Reference:** See `API_REFERENCE.md`
- **Architecture:** See `ARCHITECTURE.md`
- **Contributing:** See `CONTRIBUTING.md`

## 🆘 Support

- **Issues:** GitHub Issues
- **Discussions:** GitHub Discussions
- **Email:** support@junifydb.org

---

**Happy Coding! 🎉**