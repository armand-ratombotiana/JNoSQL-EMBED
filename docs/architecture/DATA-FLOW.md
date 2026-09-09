# JNOSQL-EMBED: Data Flow Architecture

A technical deep-dive into how data travels through JNOSQL-EMBED during read, write, query, and recovery operations.

---

## 1. Document Write Path (Insert / Update)

```
[Application]
      │
      ▼
1. DocumentCollection.insert(doc)
      │
      ├─► Emit EventBus BEFORE_INSERT
      ├─► Assign UUID if doc.id() == null
      ├─► Calculate TTL timestamp if ttlSeconds > 0
      │
      ▼
2. StorageEngine.putRecord(collection, doc)
      │
      ├─► Serialize Document to JSON / Byte payload (JsonSerde)
      ├─► Write to MemTable / ConcurrentHashMap / File WAL Channel
      │
      ▼
3. SecondaryIndex.add(doc)
      │
      ├─► Extract indexed field values
      ├─► Update Inverted Index: FieldValue -> Set<DocId>
      │
      ▼
4. QueryResultCache.invalidatePattern(collection)
      │
      ▼
5. Metrics & EventBus
      ├─► metrics.recordInsert()
      └─► emit EventBus AFTER_INSERT
```

---

## 2. Document Query & Read Path

```
[Application]
      │
      ▼
1. DocumentCollection.find(query)
      │
      ▼
2. Query Execution Strategy Selection:
      │
      ├── [Case A: Query on Indexed Field via Query.eq(field, val)]
      │         │
      │         ├─► SecondaryIndex.get(fieldValue) -> Fast Set<DocId> lookup
      │         ├─► Fetch only matching documents from StorageEngine
      │         └─► Return filtered List<Document>
      │
      └── [Case B: Non-Indexed or Complex Composite Predicate]
                │
                ├─► StorageEngine.scan(collection) -> Scan stream of documents
                ├─► Filter via Query Predicate<Document> in-memory
                ├─► Apply SortOrder, Offset, and Limit
                └─► Return List<Document>
```

---

## 3. Transaction Write & Commit Path

```
1. Transaction tx = db.beginTransaction()
      ├─► Assign logical snapshot readTimestamp from MVCCManager
      └─► Initialize private uncommitted Operation buffer

2. tx.write(collection, key, value)
      ├─► Stage write operation in local transaction buffer
      └─► (Optional MVCC stageWrite in MVCCManager)

3. tx.read(collection, key)
      ├─► Check local write buffer first (Read-Your-Own-Writes)
      └─► If not in buffer, read version at snapshot readTimestamp

4. tx.commit()
      ├─► Acquire collection-level commit lock
      ├─► Flush all staged operations sequentially into StorageEngine
      ├─► Update MVCC commit timestamp
      ├─► Emit AFTER_COMMIT on EventBus
      └─► Clear local operation buffer and mark status COMMITTED

5. tx.rollback() (if aborted)
      ├─► Discard local staged operations without touching StorageEngine
      └─► Mark status ROLLED_BACK
```

---

## 4. Crash Recovery Path (`FileEngine`)

When opening an existing database directory:
1. `FileEngine` locates the active WAL (Write-Ahead Log) file and `.db` data files.
2. The `WALManager` reads the log sequentially from the last known checkpoint.
3. For each logged `PUT` entry, the record is re-applied to the in-memory index table.
4. For each logged `DELETE` entry, the key is evicted.
5. Incomplete transactions (entries without a terminal `COMMIT` marker) are discarded.
6. The database reaches a fully consistent state and is marked open.
