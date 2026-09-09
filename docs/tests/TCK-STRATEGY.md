# JNOSQL-EMBED: Technology Compatibility Kit (TCK) Strategy

Strategy for verifying alignment with Jakarta NoSQL and JVM embedded storage contracts.

---

## 1. StorageEngine Contract Test Suite (Engine TCK)

To guarantee that all current and future `StorageEngine` implementations behave identically, an abstract contract test suite evaluates each engine against the identical behavioral test suite.

### Evaluated Invariants:
1. `put(col, key, val)` followed by `get(col, key)` must return exact value.
2. `get(col, nonExistentKey)` must return `null`.
3. `delete(col, key)` followed by `exists(col, key)` must return `false`.
4. `keys(col)` must contain all active keys and exclude deleted keys.
5. `scan(col)` must stream all active records in the collection.
6. Multi-collection isolation: mutating collection A must have zero side effects on collection B.

### Concrete Contract Test Targets:
- `InMemoryEngine`
- `FileEngine`
- `BTreeEngine`
- `LSMTreeEngine`

---

## 2. Jakarta NoSQL Alignment Verification

We validate alignment with Jakarta NoSQL concepts:
- **Document Operations**: Insertion, key lookup, field-based queries, and serialization.
- **Key-Value Operations**: Named buckets, TTL expirations, and atomic numeric mutations.
- **Column Family Operations**: Wide rows, dynamic qualifiers, and cell timestamps.

### Honest Compliance Stance:
JNOSQL-EMBED provides full conceptual alignment with Jakarta NoSQL data models and idioms, but is delivered as an independent, embedded-first engine rather than a heavy CDI-only enterprise driver container.
