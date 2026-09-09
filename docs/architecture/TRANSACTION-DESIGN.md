# JNOSQL-EMBED: Transaction & MVCC Design

A detailed examination of the transaction model, isolation levels, concurrency guarantees, and atomicity semantics in JNOSQL-EMBED.

---

## 1. Concurrency Model & Isolation Levels

JNOSQL-EMBED implements **Snapshot Isolation (SI)** powered by Multi-Version Concurrency Control (MVCC).

### Guarantees:
1. **No Dirty Reads**: A transaction never reads uncommitted changes made by other concurrent transactions.
2. **Non-Blocking Reads**: Readers never block writers, and writers never block readers.
3. **Read-Your-Own-Writes**: A transaction always sees its own uncommitted staged mutations when executing subsequent reads.
4. **Repeatable Snapshot**: All reads within a transaction observe data as it existed at the exact moment the transaction began (`readTimestamp`).

---

## 2. Transaction Architecture

The transaction subsystem comprises three primary components:

### 2.1 `Transaction.java`
An `AutoCloseable` transactional context created via `db.beginTransaction()`.
- **Status lifecycle**: `ACTIVE -> COMMITTED` or `ACTIVE -> ROLLED_BACK` or `TIMEOUT`.
- **Operation Buffer**: An in-memory sequence of `Operation(Type, collection, key, value)` recording all uncommitted actions.
- **Activity Tracker**: Tracks `lastActivity` and enforces transaction timeout (default 30,000ms) to prevent resource leaks from abandoned transactions.

### 2.2 `MVCCManager.java`
Coordinates versioning and timestamps across the engine:
- Maintains a monotonically increasing atomic logical clock for timestamps (`assignTimestamp()`).
- Stores historical versions of records indexed by commit timestamp.
- Evaluates version visibility for snapshot queries.

### 2.3 Atomicity Enforcement
```java
try (Transaction tx = db.beginTransaction()) {
    tx.write("orders", "ord-101", orderJson);
    tx.write("inventory", "sku-404", updatedInventoryJson);
    
    // Read-your-own-writes check
    String stagedOrder = tx.read("orders", "ord-101");
    
    tx.commit(); // Atomic write to underlying StorageEngine
} catch (Exception e) {
    // If commit() is not called before close(), auto-rollback triggers
}
```

---

## 3. Failure Modes & Recovery

| Failure Scenario | Engine Behavior | Durability Guarantee |
|---|---|---|
| **Exception before commit()** | Staged operation buffer is discarded; underlying storage remains completely untouched. | Complete atomicity (0% partial write). |
| **JVM Crash during active transaction** | Uncommitted buffer exists only in volatile RAM; persistent storage has zero dirty records. | Consistent rollback on restart. |
| **JVM Crash during commit() flush** | `FileEngine` WAL logs the transaction boundary. If the terminal `COMMIT` marker was not flushed to disk before power loss, replay ignores the partial records. | Crash-consistent recovery via WAL replay. |
| **Transaction Thread Abandonment** | Timeout monitor flags transactions exceeding `timeoutMs` as `TIMEOUT` and releases staged memory. | Leak prevention in long-running servers. |
