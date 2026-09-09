# Demonstration Validation Matrix

This matrix documents the real test execution results across all demonstration applications and frameworks in the JunifyDB ecosystem.

## Framework Execution Matrix

| Demo Application | Framework & Version | Integration Mechanism | Test Class | Tests Run | Failures | Errors | Status | Execution Time |
|---|---|---|---|---|---|---|---|---|
| **[spring-boot-demo](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo/spring-boot-demo)** | Spring Boot 3.2.0 | `junify-db-spring-boot-starter` | `EcommerceApplicationTest` | 3 | 0 | 0 | **PASS** | 2.65 s |
| **[quarkus-demo](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo/quarkus-demo)** | Quarkus 3.8.0 | `junify-db-quarkus-extension-runtime` | `ProductResourceTest` | 4 | 0 | 0 | **PASS** | 4.49 s |
| **[micronaut-demo](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo/micronaut-demo)** | Micronaut 4.2.0 | `junifydb-micronaut-integration` | `EcommerceControllerTest` | 4 | 0 | 0 | **PASS** | 2.22 s |
| **[vertx-demo](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo/vertx-demo)** | Eclipse Vert.x 4.5.4 | `junify-db-core` (`executeBlocking`) | `EcommerceVerticleTest` | 4 | 0 | 0 | **PASS** | 1.44 s |
| **[end-to-end-validation](file:///c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/demo/end-to-end-validation)** | JUnit 5.10.2 | Native Multi-Engine Matrix | `MultiEngineE2EValidationTest` | 4 | 0 | 0 | **PASS** | 0.60 s |

---

## Multi-Engine Capabilities Matrix

| Storage Engine | Documents (`products`/`orders`) | Key-Value (`price_cache`) | Wide-Column (`inventory`) | MVCC ACID Transactions | Restart Durability | Status |
|---|---|---|---|---|---|---|
| `IN_MEMORY` | Yes | Yes | Yes | Yes | N/A (RAM-only) | **VERIFIED** |
| `FILE` | Yes | Yes | Yes | Yes | Verified | **VERIFIED** |
| `B_TREE` | Yes | Yes | Yes | Yes | Verified | **VERIFIED** |
| `LSM_TREE` | Yes | Yes | Yes | Yes | Verified | **VERIFIED** |

---

## Verification Evidence Log
- All demo sub-modules compile and execute using standard `mvn clean test` commands.
- Zero mock libraries: real disk files and actual in-process memory structures are allocated, asserted, and closed cleanly.
- Transaction abort scenarios verify complete isolation: inventory levels never corrupt upon rollback.
