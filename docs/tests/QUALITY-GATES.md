# JNOSQL-EMBED: Quality Gates & CI Standards

Standardized quality criteria enforced on pull requests, commits, and releases.

---

## 1. Quality Gates Matrix

| Gate | Tool / Mechanism | Threshold | Enforcement |
|---|---|---|---|
| **Build Compilation** | Maven (`javac -release 17`) | 0 compilation errors, 0 deprecation warnings on public APIs | Mandatory on every commit |
| **Unit Test Suite** | Maven Surefire | 100% passing tests (0 failures, 0 errors, 0 skipped without reason) | Mandatory on every commit |
| **Spring Boot Tests** | Spring Boot Test runner | 100% passing (12/12) | Mandatory on every commit |
| **Quarkus Compilation** | Quarkus BOM 3.8+ compiler | 0 errors in runtime or deployment modules | Mandatory on every commit |
| **Micronaut Compilation** | Micronaut Annotation Processor | 0 errors in bean generation | Mandatory on every commit |
| **Code Coverage Gate** | JaCoCo Maven Plugin | Enforced in CI via `-P coverage-check` | Advisory locally, gate in CI |
| **Clean Working Tree** | Git | 0 untracked build artifacts, 0 temporary files | Mandatory before release |

---

## 2. Command Reference for Quality Verification

```bash
# 1. Compile all core classes
mvn clean compile

# 2. Run all core tests
mvn test

# 3. Test Spring Boot integration
cd spring-boot-starter && mvn test && cd ..

# 4. Compile Quarkus extension
cd quarkus-extension && mvn clean compile && cd ..

# 5. Compile Micronaut integration
cd micronaut-integration && mvn clean compile && cd ..

# 6. Execute standalone feature verification program
mvn exec:java
```
