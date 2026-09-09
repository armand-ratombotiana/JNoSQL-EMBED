# JNOSQL-EMBED: Test Gaps & Blind Spots

Identified testing gaps and actionable plans to remediate them.

---

## 1. Identified Test Gaps

### 1.1 Disk-Full / Out-of-Space Simulation
- **Current State**: Tests assume unbounded temporary disk space in `target/`.
- **Gap**: How does `FileEngine` or `BTreeEngine` react when `write()` encounters `IOException: No space left on device`?
- **Remediation**: Add a simulated restricted-capacity `FileSystem` via Jimfs or mock channel to assert clean error propagation without database corruption.

### 1.2 Corrupted WAL Replay Handling
- **Current State**: `FilePersistenceTest` verifies recovery from clean WAL logs.
- **Gap**: Verifying behavior when a WAL log contains truncated or bit-flipped bytes midway through a transaction.
- **Remediation**: Author test inserting intentionally truncated WAL bytes and verifying recovery cleanly skips incomplete records.

### 1.3 Reactive Vert.x Non-Blocking Thread Verification
- **Current State**: Vert.x usage was unvalidated in the main repository.
- **Gap**: Ensuring blocking database calls are never dispatched on the Vert.x event-loop thread.
- **Remediation**: Included as part of the new `demo/vertx-demo` test suite.

---

## 2. Prioritized Remediation Roadmap

1. **High Priority**: Author the Vert.x demo with worker-thread offloading and event-loop thread assertion.
2. **Medium Priority**: Add truncated WAL recovery test in `FilePersistenceTest`.
3. **Low Priority**: Add simulated disk-full I/O test suite.
