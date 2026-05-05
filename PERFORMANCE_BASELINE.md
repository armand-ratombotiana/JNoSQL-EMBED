# JunifyDB Performance Baseline Report

**Generated:** 2026-05-04  
**Java Version:** 25  
**JVM:** Eclipse Temurin 25  
**SPEC.md Version:** 1.0.0

---

## Executive Summary

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Startup Time | <200ms | _PENDING_ | ⏳ |
| KV p50 Latency | <1ms | _PENDING_ | ⏳ |
| Indexed p50 Latency | <5ms | _PENDING_ | ⏳ |
| Hybrid/Vector p99 | <50ms | _PENDING_ | ⏳ |
| Throughput | >50K ops/sec | _PENDING_ | ⏳ |
| Heap (idle) | <50MB | _PENDING_ | ⏳ |
| GC Pause | <10ms | _PENDING_ | ⏳ |

---

## Storage Engine Benchmarks

### In-Memory Engine

| Operation | Throughput (ops/sec) | p50 (ms) | p99 (ms) |
|-----------|---------------------|----------|----------|
| KV Get | _PENDING_ | _PENDING_ | _PENDING_ |
| KV Put | _PENDING_ | _PENDING_ | _PENDING_ |
| KV Put with TTL | _PENDING_ | _PENDING_ | _PENDING_ |
| KV Increment | _PENDING_ | _PENDING_ | _PENDING_ |
| Document Insert | _PENDING_ | _PENDING_ | _PENDING_ |
| Document Get By ID | _PENDING_ | _PENDING_ | _PENDING_ |
| Document Find All | _PENDING_ | _PENDING_ | _PENDING_ |
| Document Find By Query | _PENDING_ | _PENDING_ | _PENDING_ |
| Batch Insert (10) | _PENDING_ | _PENDING_ | _PENDING_ |

### File Engine (Sync)

| Operation | Throughput (ops/sec) | p50 (ms) | p99 (ms) |
|-----------|---------------------|----------|----------|
| KV Get | _PENDING_ | _PENDING_ | _PENDING_ |
| KV Put | _PENDING_ | _PENDING_ | _PENDING_ |
| Document Insert | _PENDING_ | _PENDING_ | _PENDING_ |
| Document Get By ID | _PENDING_ | _PENDING_ | _PENDING_ |

### File Engine (Async)

| Operation | Throughput (ops/sec) | p50 (ms) | p99 (ms) |
|-----------|---------------------|----------|----------|
| KV Put | _PENDING_ | _PENDING_ | _PENDING_ |
| Document Insert | _PENDING_ | _PENDING_ | _PENDING_ |

### B-Tree Engine

| Operation | Throughput (ops/sec) | p50 (ms) | p99 (ms) |
|-----------|---------------------|----------|----------|
| KV Get | _PENDING_ | _PENDING_ | _PENDING_ |
| KV Put | _PENDING_ | _PENDING_ | _PENDING_ |
| Document Insert | _PENDING_ | _PENDING_ | _PENDING_ |
| Range Query | _PENDING_ | _PENDING_ | _PENDING_ |

### LSM-Tree Engine

| Operation | Throughput (ops/sec) | p50 (ms) | p99 (ms) |
|-----------|---------------------|----------|----------|
| KV Put | _PENDING_ | _PENDING_ | _PENDING_ |
| KV Get | _PENDING_ | _PENDING_ | _PENDING_ |
| Document Insert | _PENDING_ | _PENDING_ | _PENDING_ |
| Compaction Overhead | _PENDING_ | _PENDING_ | _PENDING_ |

### H2 SQL Engine

| Operation | Throughput (ops/sec) | p50 (ms) | p99 (ms) |
|-----------|---------------------|----------|----------|
| SQL SELECT | _PENDING_ | _PENDING_ | _PENDING_ |
| SQL INSERT | _PENDING_ | _PENDING_ | _PENDING_ |
| SQL JOIN | _PENDING_ | _PENDING_ | _PENDING_ |

---

## MVCC Transaction Benchmarks

| Operation | Latency (μs) | p50 | p99 |
|-----------|-------------|-----|-----|
| Transaction Start/Commit | _PENDING_ | _PENDING_ | _PENDING_ |
| Read Isolation | _PENDING_ | _PENDING_ | _PENDING_ |
| Write with Commit | _PENDING_ | _PENDING_ | _PENDING_ |
| Rollback | _PENDING_ | _PENDING_ | _PENDING_ |
| Timestamp Allocation | _PENDING_ | _PENDING_ | _PENDING_ |

**Target:** Transaction commit overhead <100μs

---

## WAL Benchmarks

| Mode | Operation | Throughput (ops/sec) | Latency (μs) |
|------|-----------|---------------------|--------------|
| Sync | WAL Append | _PENDING_ | _PENDING_ |
| Sync | Flush | _PENDING_ | _PENDING_ |
| Async | WAL Append | _PENDING_ | _PENDING_ |
| Async | Flush | _PENDING_ | _PENDING_ |

**Targets:**
- WAL append >100K ops/sec
- Async flush latency <1ms
- Sync flush latency <5ms

---

## Vector Search Benchmarks (HNSW)

### 128 Dimensions

| Operation | Vector Count | Latency (ms) |
|-----------|-------------|--------------|
| Search Top-10 | 1,000 | _PENDING_ |
| Search Top-10 | 10,000 | _PENDING_ |
| Search Top-100 | 1,000 | _PENDING_ |
| Search Top-100 | 10,000 | _PENDING_ |
| Insert | - | _PENDING_ |

### 256 Dimensions

| Operation | Vector Count | Latency (ms) |
|-----------|-------------|--------------|
| Search Top-10 | 1,000 | _PENDING_ |
| Search Top-10 | 10,000 | _PENDING_ |

### 512 Dimensions

| Operation | Vector Count | Latency (ms) |
|-----------|-------------|--------------|
| Search Top-10 | 1,000 | _PENDING_ |
| Search Top-10 | 10,000 | _PENDING_ |

### 1024 Dimensions

| Operation | Vector Count | Latency (ms) |
|-----------|-------------|--------------|
| Search Top-10 | 1,000 | _PENDING_ |
| Search Top-10 | 10,000 | _PENDING_ |

**Target:** Sub-millisecond similarity search for 100K vectors

---

## JVM Performance Metrics

### Memory

| Metric | Target | Actual |
|--------|--------|--------|
| Max Heap | - | _PENDING_ MB |
| Idle Heap | <50MB | _PENDING_ MB |
| Used Heap (loaded) | - | _PENDING_ MB |

### GC

| Metric | Target | Actual |
|--------|--------|--------|
| Young Gen Pause | <10ms | _PENDING_ ms |
| Old Gen Frequency | - | _PENDING_ /min |
| GC Throughput | >95% | _PENDING_ % |

### Threads

| Metric | Virtual Threads | Platform Threads |
|--------|----------------|------------------|
| Active (idle) | _PENDING_ | _PENDING_ |
| Active (load) | _PENDING_ | _PENDING_ |

---

## JFR Profiling Results

### CPU Profile

**Recording:** `target/jfr/storage.jfr`

**Hot Methods:**
1. _PENDING_
2. _PENDING_
3. _PENDING_

### Memory Allocation Profile

**Top Allocating Methods:**
1. _PENDING_
2. _PENDING_
3. _PENDING_

### Lock Contention

**Lock Contention Events:** _PENDING_

**Hot Locks:**
1. _PENDING_
2. _PENDING_

---

## async-profiler Results

### CPU Flame Graph

**Output:** `target/async-profiler/cpu.html`

**Key Observations:**
- _PENDING_

### Memory Allocation Flame Graph

**Output:** `target/async-profiler/alloc.html`

**Key Observations:**
- _PENDING_

---

## Comparison: Java 17 vs Java 25

| Metric | Java 17 | Java 25 | Improvement |
|--------|---------|---------|-------------|
| Startup Time | _PENDING_ | _PENDING_ | _PENDING_ |
| KV Throughput | _PENDING_ | _PENDING_ | _PENDING_ |
| p99 Latency | _PENDING_ | _PENDING_ | _PENDING_ |
| GC Pause | _PENDING_ | _PENDING_ | _PENDING_ |

---

## Recommendations

### Performance Optimizations

1. _PENDING_
2. _PENDING_
3. _PENDING_

### Bottlenecks Identified

1. _PENDING_
2. _PENDING_
3. _PENDING_

### Next Steps

1. _PENDING_
2. _PENDING_
3. _PENDING_

---

## Appendix: Benchmark Configuration

### JVM Flags

```
--enable-preview
-XX:+UseG1GC
-XX:MaxGCPauseMillis=10
-XX:+UnlockDiagnosticVMOptions
-XX:+DebugNonSafepoints
```

### JMH Configuration

```
Warmup: 3 iterations, 500ms each
Measurement: 5 iterations, 1s each
Forks: 1
Time Unit: microseconds/ms/seconds (varies by benchmark)
```

### Hardware

- **CPU:** _PENDING_
- **Memory:** _PENDING_
- **OS:** _PENDING_
- **Disk:** _PENDING_

---

**Report Generated:** 2026-05-04  
**Next Baseline:** After JPA 3.1 completion
