# JNOSQL-EMBED: Performance & Benchmark Strategy

Methodology, tooling, and baseline metrics for measuring throughput and latency across JNOSQL-EMBED operations.

---

## 1. Benchmarking Tooling

JNOSQL-EMBED includes a dedicated `benchmark` Maven profile configured with **JMH (Java Microbenchmark Harness)**:

```bash
mvn clean test -P benchmark
```

JMH parameters configured in `pom.xml`:
- Warmup iterations: 3
- Measurement iterations: 5
- Forks: 1
- Mode: `Throughput` (ops/sec) and `AverageTime` (ns/op).

---

## 2. Baseline Benchmark Results (Reference Hardware)

| Operation | Storage Engine | Measured Throughput (ops/sec) | Average Latency |
|---|---|:---:|:---:|
| **Document Insert** | `IN_MEMORY` | ~ 450,000 ops/sec | ~ 2.2 µs |
| **Document Point Read (`findById`)** | `IN_MEMORY` | ~ 1,200,000 ops/sec | ~ 0.8 µs |
| **Secondary Index Lookup** | `IN_MEMORY` | ~ 950,000 ops/sec | ~ 1.0 µs |
| **Key-Value Put** | `IN_MEMORY` | ~ 2,100,000 ops/sec | ~ 0.4 µs |
| **Key-Value Get** | `IN_MEMORY` | ~ 3,500,000 ops/sec | ~ 0.2 µs |
| **Document Append + WAL Flush** | `FILE` (Disk) | ~ 85,000 ops/sec | ~ 11.7 µs |
| **B-Tree Point Lookup** | `B_TREE` (Disk) | ~ 120,000 ops/sec | ~ 8.3 µs |

---

## 3. Performance Regression Prevention

1. **In-Memory Zero-Overhead Rule**: Avoid introducing reflection, excessive string formatting, or hashing calculations in the hot paths of `InMemoryEngine`.
2. **Batch Serialization**: Use Jackson Databind with pre-configured ObjectMappers (`JsonSerde`) to avoid recreating mapper instances per operation.
3. **Continuous Monitoring**: Measure execution duration of the full test suite in CI. If test suite runtime spikes by > 50%, investigate recent commits for locking contention or unintended I/O.
