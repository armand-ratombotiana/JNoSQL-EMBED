---
name: Java 25 Performance Requirements
description: SPEC.md mandates Java 25 with virtual threads, Panama FFM, structured concurrency for JVM-optimized performance
type: feedback
---

## Java 25 Performance Requirements

**Why:** SPEC.md defines JunifyDB as a next-generation embedded database with JVM-optimized performance targets (startup <200ms, p50 <1ms for KV, >50K ops/sec throughput). Java 25 features are essential architectural requirements, not optional enhancements.

**How to apply:**
- All new code must target Java 25 (`javac --release 25`)
- Use virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`) for I/O operations
- Use structured concurrency for batch operations
- Use Panama FFM for off-heap buffers in storage/WAL layer
- Use scoped values for transaction context propagation
- Prefer records and sealed types for data modeling
- Enable escape analysis and stack allocation patterns
- Minimize object churn in hot paths

**Performance gates before Git commit:**
- JMH benchmarks pass with expected p50/p99/throughput metrics
- JVM performance test suite validates latency, throughput, GC impact
- Zero lock contention on read paths
- Heap <50MB idle, GC pause <10ms
