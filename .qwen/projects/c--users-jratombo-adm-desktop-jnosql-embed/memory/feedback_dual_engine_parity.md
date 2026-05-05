---
name: Dual-Engine Parity Requirement
description: SQL and NoSQL engines must be developed, exposed, and tested in parallel with unified kernel serving both equally
type: feedback
---

## Dual-Engine Parity Requirement

**Why:** JunifyDB's value proposition is unifying SQL and NoSQL over a single shared kernel (storage, MVCC, WAL, indexing). Divergence between engines creates technical debt and undermines the unified architecture.

**How to apply:**
- Track `engine_parity` state in every checkpoint
- HALT and refactor if logic diverges between SQL/NoSQL
- Both engines must consume shared kernel via same contracts
- UnifiedRecord is the single data representation
- Every feature exposed for SQL must have NoSQL equivalent (and vice versa)
- Console must provide unified query editor (SQL/NoSQL/Hybrid tabs)
- Benchmarks must measure both engines equally

**Parity checkpoints:**
- Storage: Both engines use same StorageEngine SPI
- Transactions: Both use same MVCCManager
- WAL: Both write to same WAL format
- Indexing: Both use same index implementations
- Metrics: Both report to same DatabaseMetrics

**Before Git commit:**
- `engine_parity.sql` = PASS
- `engine_parity.nosql` = PASS
- No duplicated logic between engines
- Shared kernel integrity verified
