# JNOSQL-EMBED: Missing Features & Future Roadmap

Features identified as high-value additions for future release cycles, currently not yet implemented.

---

## 1. Zero-Copy Off-Heap Direct Byte Storage
- **Description**: Storage engine option utilizing `sun.misc.Unsafe` or Java 22+ Foreign Function & Memory API (`Arena`, `MemorySegment`) to store key-value buffers outside the Java garbage collected heap.
- **Value**: Enables storing multi-gigabyte datasets in-memory without contributing to JVM GC pause times.
- **Priority**: Medium (Planned for v1.2.0).

---

## 2. Text Search Stemming & Stop-Words
- **Description**: While `TextSearchTest` and basic word tokenization exist, linguistic stemming (Porter Stemmer) and language-specific stop-word removal are not yet present.
- **Value**: Improves full-text search relevance for textual document fields.
- **Priority**: Low (Planned for v1.3.0).

---

## 3. Schema Validation Rules (JSON Schema)
- **Description**: Enforcing optional strict or advisory JSON schema validation on document collections upon `insert()` or `update()`.
- **Value**: Prevents malformed or schema-incompatible documents from being committed by client applications.
- **Priority**: Medium (Planned for v1.1.0).
