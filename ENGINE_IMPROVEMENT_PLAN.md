# JUNIFYDB DUAL-ENGINE FIX & IMPROVEMENT PLAN

**Based on**: ENGINE_VALIDATION_REPORT.md  
**Date**: 2026-05-06  
**Status**: READY FOR EXECUTION

---

## EXECUTIVE SUMMARY

This plan addresses all identified issues and improvement opportunities for both the Relational (H2) and Non-Relational (IN_MEMORY) engines. The plan is organized into phases with clear validation proofs for each feature.

---

## PHASE 1: CRITICAL FIXES (Schema API)

### Issue: Schema API returns empty results on both engines

**Root Cause Analysis Required**:
- SchemaManager.getTables() returning empty
- SchemaManager.getColumns() returning empty
- SchemaManager.getColumnType() returning VARCHAR instead of actual type

**Files to Fix**:
1. `src/main/java/org/junify/db/storage/spi/SchemaManager.java`
2. `src/main/java/org/junify/db/storage/spi/H2StorageEngine.java`
3. `src/main/java/org/junify/db/console/http/JunifyDBServer.java` (SchemaHandler)

**Validation Tests**:
```bash
# Before Fix
GET /api/schema → (empty)
GET /api/tables/products → {"columns":[],"name":"products"}

# After Fix (Expected)
GET /api/schema → {"tables":["products","users"]}
GET /api/tables/products → {"columns":[{"name":"id","type":"INT"},{"name":"name","type":"VARCHAR"}]}
```

---

## PHASE 2: RELATIONAL ENGINE (H2) IMPROVEMENTS

### 2.1 Enhanced SQL Support

**Current State**: Basic SQL working
**Improvements Needed**:
- [ ] Prepared statements support
- [ ] Transaction isolation levels
- [ ] Foreign key constraints
- [ ] Index creation via SQL

**Validation Proof**:
```sql
-- Test prepared statements
POST /api/sql "SELECT * FROM users WHERE age > ?" [18]

-- Test foreign keys
POST /api/sql "ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)"
```

### 2.2 Schema Browser Enhancement

**Current State**: Empty results
**Target State**: Full schema visualization

**Validation Proof**:
```bash
GET /api/schema → {"tables":["users","products","orders"]}
GET /api/tables/users → {
  "columns": [
    {"name":"id","type":"INT","primaryKey":true},
    {"name":"name","type":"VARCHAR(100)"},
    {"name":"email","type":"VARCHAR(255)"}
  ]
}
```

### 2.3 H2 Performance Optimization

**Improvements**:
- [ ] Connection pooling optimization
- [ ] Query plan caching
- [ ] Batch insert optimization

**Validation Proof**:
```bash
# Measure insert latency before/after
ab -n 1000 -c 10 http://localhost:14002/api/sql
Expected: p50 < 5ms, p99 < 20ms
```

---

## PHASE 3: NON-RELATIONAL ENGINE (IN_MEMORY) IMPROVEMENTS

### 3.1 Enhanced Document Operations

**Current State**: Basic CRUD working
**Improvements Needed**:
- [ ] Advanced query operators ($regex, $in, $nin)
- [ ] Document validation with schema
- [ ] TTL (Time-To-Live) for documents
- [ ] Full-text search integration

**Validation Proof**:
```bash
# Test regex query
POST /api/collections/users/query
Body: {"name": {"$regex": "^A.*"}}

# Test TTL
POST /api/collections/sessions
Body: {"user_id":"123","expires_in":3600}
```

### 3.2 Key-Value Enhancements

**Current State**: Basic operations + batch working
**Improvements Needed**:
- [ ] Atomic increment/decrement
- [ ] List/Set data structures
- [ ] Hash operations
- [ ] Expiration callbacks

**Validation Proof**:
```bash
# Test atomic increment
POST /api/kv/counters/page_views/increment → {"value":1001}

# Test list operations
POST /api/kv/lists/tags/push → {"value":"tag1"}
GET /api/kv/lists/tags → {"values":["tag1","tag2"]}
```

### 3.3 Column-Family Enhancements

**Current State**: Basic row operations working
**Improvements Needed**:
- [ ] Column-level TTL
- [ ] Wide-column pagination
- [ ] Column family statistics

**Validation Proof**:
```bash
# Test column TTL
PUT /api/columns/sessions/user1
Body: {"token":"abc123","ttl":3600}

# Test pagination
GET /api/columns/logs/batch1?limit=100&offset=0
```

---

## PHASE 4: CROSS-ENGINE FEATURES

### 4.1 Unified Query Interface

**Goal**: Single API for both engines

**Validation Proof**:
```bash
# Auto-route to appropriate engine
POST /api/query
Body: {
  "engine": "auto",
  "type": "document",
  "collection": "users",
  "filter": {"age": {"$gt": 18}}
}
```

### 4.2 Hybrid Transactions

**Goal**: Transaction support across both engines

**Validation Proof**:
```bash
# Begin hybrid transaction
POST /api/transactions/hybrid
Body: {"engines":["H2","IN_MEMORY"]}

# Operations in both engines
POST /api/transactions/{id}/operations
Body: [
  {"engine":"H2","sql":"INSERT INTO..."},
  {"engine":"IN_MEMORY","collection":"users","op":"insert"}
]

# Commit both or rollback both
POST /api/transactions/{id}/commit
```

### 4.3 Data Migration Tools

**Goal**: Move data between engines

**Validation Proof**:
```bash
# Migrate H2 table to document collection
POST /api/migrate
Body: {
  "from":{"engine":"H2","table":"users"},
  "to":{"engine":"IN_MEMORY","collection":"users"}
}
→ {"migrated":100,"status":"success"}
```

---

## PHASE 5: OBSERVABILITY & MONITORING

### 5.1 Enhanced Metrics

**Improvements**:
- [ ] Per-engine metrics separation
- [ ] Query latency histograms
- [ ] Connection pool metrics
- [ ] Cache hit/miss rates

**Validation Proof**:
```bash
GET /api/metrics/detailed
→ {
  "H2": {"queries":{"p50":2,"p99":15},"connections":{"active":5,"idle":10}},
  "IN_MEMORY": {"reads":{"p50":0.5,"p99":2},"writes":{"p50":1,"p99":5}}
}
```

### 5.2 Query Profiler

**Improvements**:
- [ ] Execution plan display
- [ ] Slow query log
- [ ] Index usage analysis

**Validation Proof**:
```bash
POST /api/sql/profiler
Body: "SELECT * FROM users WHERE email = 'test@example.com'"
→ {
  "plan":"INDEX_SCAN (idx_email)",
  "estimatedRows":1,
  "actualRows":1,
  "executionTime":"0.5ms"
}
```

---

## VALIDATION MATRIX

| Phase | Feature | IN_MEMORY | H2 | Validation Method |
|-------|---------|-----------|-----|-------------------|
| 1 | Schema API | ⚠️→✅ | ⚠️→✅ | API call + Test |
| 2.1 | Enhanced SQL | N/A | ✅→🚀 | SQL queries |
| 2.2 | Schema Browser | ⚠️→✅ | ⚠️→✅ | UI verification |
| 2.3 | H2 Performance | N/A | ✅→🚀 | ab benchmark |
| 3.1 | Document Ops | ✅→🚀 | N/A | Query tests |
| 3.2 | KV Enhancements | ✅→🚀 | N/A | API tests |
| 3.3 | Column-Family | ✅→🚀 | N/A | API tests |
| 4.1 | Unified Query | ✅ | ✅ | Cross-engine test |
| 4.2 | Hybrid Transactions | ✅ | ✅ | Transaction test |
| 4.3 | Data Migration | ✅ | ✅ | Migration test |
| 5.1 | Enhanced Metrics | ✅ | ✅ | Metrics endpoint |
| 5.2 | Query Profiler | N/A | ✅ | Profiler test |

**Legend**: ✅ Working | ⚠️ Issues | 🚀 Enhanced | N/A Not Applicable

---

## EXECUTION STRATEGY

### Multi-Agent Approach

**Agent 1 - Schema Analyst**:
- Analyze SchemaManager.java line by line
- Identify why getTables() returns empty
- Identify why getColumns() returns empty
- Report root cause with code references

**Agent 2 - Schema Fixer**:
- Implement fixes based on Agent 1 analysis
- Update SchemaManager methods
- Update SchemaHandler in JunifyDBServer
- Run SchemaManagerTest to verify

**Agent 3 - H2 Specialist**:
- Enhance SQL support (prepared statements, constraints)
- Optimize connection pooling
- Add query plan caching
- Test with SQL benchmark suite

**Agent 4 - NoSQL Specialist**:
- Add advanced document query operators
- Enhance KV with data structures
- Add column-family pagination
- Test with NoSQL benchmark suite

**Agent 5 - Validation Engineer**:
- Create comprehensive test script
- Test all 13 tabs on both engines
- Document proof of each feature
- Generate validation report

---

## SUCCESS CRITERIA

### Phase 1 (Schema API) - CRITICAL
- [ ] GET /api/schema returns table list
- [ ] GET /api/tables/{name} returns columns with types
- [ ] SchemaManagerTest: 6/6 tests passing
- [ ] Schema Browser tab shows data in UI

### Phase 2-5 (Improvements) - IMPORTANT
- [ ] All validation proofs pass
- [ ] Test coverage: 98%+ (137/140 tests)
- [ ] Performance benchmarks met
- [ ] Documentation updated

---

## TIMELINE

| Phase | Estimated Time | Dependencies |
|-------|---------------|--------------|
| 1 - Schema Fix | 2-3 hours | None |
| 2 - H2 Improvements | 3-4 hours | Phase 1 |
| 3 - NoSQL Improvements | 3-4 hours | Phase 1 |
| 4 - Cross-Engine | 4-5 hours | Phase 2, 3 |
| 5 - Observability | 2-3 hours | Phase 2, 3 |
| **Total** | **14-19 hours** | Sequential + Parallel |

---

## RISK ASSESSMENT

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Schema fix breaks existing tests | Low | High | Run full test suite after each change |
| H2 performance regression | Medium | Medium | Benchmark before/after each change |
| Cross-engine complexity | High | Medium | Start with simple unified interface |
| Timeline overrun | Medium | Low | Prioritize Phase 1 (critical fixes) |

---

## NEXT STEPS

1. **IMMEDIATE**: Launch Agent 1 to analyze SchemaManager
2. **SHORT-TERM**: Fix Schema API (Phase 1)
3. **MEDIUM-TERM**: Implement improvements (Phase 2-5)
4. **LONG-TERM**: Production deployment with full validation

---

**Plan Approved**: Ready for multi-agent execution
**Validation Standard**: Proof required for each feature
**Documentation**: All changes documented in ENGINE_IMPROVEMENTS.md
