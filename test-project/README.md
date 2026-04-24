# JunifyDB Test Projects

Test projects for verifying JPA/Hibernate and Jakarta NoSQL specification compliance.

## Projects

| Project | Framework | Tests |
|---------|----------|-------|
| `simple-java` | Plain Java (JUnit 5) | JPA EntityManager, Repository, Template |
| `spring-boot` | Spring Boot 3.x | @EntityScan, @EnableJpaRepositories, @EnableNoSqlRepositories |
| `quarkus` | Quarkus 3.x | Panache, REST endpoints, CDI |
| `micronaut` | Micronaut 4.x | Data JPA, HTTP server |

## Entity Classes

All projects use the same `User` entity class with:
- `@Entity`, `@Table` (JPA)
- `@Id`, `@GeneratedValue` (JPA + Jakarta NoSQL)
- `@Column`, `@Version`
- Jakarta NoSQL: `@Entity`, `@Id`, `@Column`

## Build & Run

### Simple Java
```bash
cd test-project/simple-java
mvn clean compile exec:java -Dexec.mainClass=test.JPATest
mvn exec:java -Dexec.mainClass=test.JakartaNoSQLTest
```

### Spring Boot
```bash
cd test-project/spring-boot
mvn spring-boot:run
```

### Quarkus
```bash
cd test-project/quarkus
mvn quarkus:dev
```

### Micronaut
```bash
cd test-project/micronaut
mvn micronaut:run
```

## Test Coverage

### JPA/Hibernate
- EntityManager (persist, find, remove, merge)
- Repository CRUD operations
- Query methods (findByField, findByFieldGreaterThan, etc.)
- Named queries (@NamedQuery)
- Version concurrency control

### Jakarta NoSQL
- Template (insert, update, find, delete)
- DocumentCollectionManager
- KeyValueTemplate (get, put, delete)
- Entity mapping with @Id, @Column

### Spring Boot Integration
- @EntityScan
- @EnableJpaRepositories
- @EnableNoSqlRepositories
- @PersistenceContext EntityManager injection

### Quarkus Integration
- @ApplicationScoped CDI
- PanacheEntity
- REST endpoints

### Micronaut Integration
- @Controller
- @Inject dependency injection
- Data JPA repositories