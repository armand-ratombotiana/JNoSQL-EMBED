package test;

import test.entity.User;
import org.junify.db.JunifyDB;
import org.junify.db.adapter.jpa.JunifyDBRepository;
import org.junify.db.adapter.jpa.JunifyDBEntityManager;
import org.junify.db.config.JunifyDBConfig;
import jakarta.persistence.*;
import java.util.List;
import java.util.Optional;

public class JPATest {

    public static void main(String[] args) {
        System.out.println("=== JPA/Hibernate Specification Test ===\n");

        var config = JunifyDBConfig.builder()
            .storageEngine(JunifyDBConfig.StorageEngineType.IN_MEMORY)
            .build();
        
        var db = JunifyDB.create(config);
        
        testEntityManager(db);
        testRepository(db);
        testQueryMethods(db);
        
        db.close();
        
        System.out.println("\n=== All JPA Tests Passed ===");
    }

    private static void testEntityManager(JunifyDB db) {
        System.out.println("1. Testing EntityManager...");
        
        var em = new JunifyDBEntityManager(db, "test-pu");
        
        var user = new User("John Doe", "john@example.com", 30);
        em.persist(user);
        
        var found = em.find(User.class, user.getId());
        if (found == null) {
            throw new RuntimeException("Failed: Entity not persisted");
        }
        
        System.out.println("   persist + find: OK");
        
        var user2 = new User("Jane Doe", "jane@example.com", 25);
        user2.setId("jane-1");
        em.persist(user2);
        
        var all = em.findAll(User.class);
        if (all.size() < 2) {
            throw new RuntimeException("Failed: findAll not working");
        }
        
        System.out.println("   findAll: OK");
        
        em.remove(user);
        var removed = em.find(User.class, user.getId());
        if (removed != null) {
            throw new RuntimeException("Failed: remove not working");
        }
        
        System.out.println("   remove: OK");
        System.out.println("   EntityManager: PASSED");
    }

    private static void testRepository(JunifyDB db) {
        System.out.println("\n2. Testing JunifyDBRepository...");
        
        var userRepo = new JunifyDBRepository<>(db, User.class, String.class);
        
        var user = new User("Alice", "alice@test.com", 28);
        user.setId("alice-1");
        
        var saved = userRepo.save(user);
        if (saved == null) {
            throw new RuntimeException("Failed: save returned null");
        }
        
        System.out.println("   save: OK");
        
        var found = userRepo.findById("alice-1");
        if (!found.isPresent()) {
            throw new RuntimeException("Failed: findById not working");
        }
        
        System.out.println("   findById: OK");
        
        var user3 = new User("Bob", "bob@test.com", 35);
        userRepo.save(user3);
        
        var all = userRepo.findAll();
        if (all.isEmpty()) {
            throw new RuntimeException("Failed: findAll not working");
        }
        
        System.out.println("   findAll: OK");
        
        var byEmail = userRepo.findByField("email", "bob@test.com");
        if (byEmail.isEmpty()) {
            throw new RuntimeException("Failed: findByField not working");
        }
        
        System.out.println("   findByField: OK");
        
        System.out.println("   Repository: PASSED");
    }

    private static void testQueryMethods(JunifyDB db) {
        System.out.println("\n3. Testing Query Methods...");
        
        var userRepo = new JunifyDBRepository<>(db, User.class, String.class);
        
        var users = List.of(
            new User("User1", "u1@test.com", 20),
            new User("User2", "u2@test.com", 25),
            new User("User3", "u3@test.com", 30),
            new User("User4", "u4@test.com", 35)
        );
        
        for (var u : users) {
            u.setId(java.util.UUID.randomUUID().toString());
            userRepo.save(u);
        }
        
        var young = userRepo.findByFieldLessThan("age", 25);
        if (young.size() != 1) {
            throw new RuntimeException("Failed: findByFieldLessThan");
        }
        
        System.out.println("   findByFieldLessThan: OK");
        
        var old = userRepo.findByFieldGreaterThan("age", 30);
        if (old.size() != 1) {
            throw new RuntimeException("Failed: findByFieldGreaterThan");
        }
        
        System.out.println("   findByFieldGreaterThan: OK");
        
        var mid = userRepo.findByFieldBetween("age", 25, 30);
        if (mid.size() != 2) {
            throw new RuntimeException("Failed: findByFieldBetween");
        }
        
        System.out.println("   findByFieldBetween: OK");
        
        var contains = userRepo.findByFieldContaining("email", "@test.com");
        if (contains.size() < 4) {
            throw new RuntimeException("Failed: findByFieldContaining");
        }
        
        System.out.println("   findByFieldContaining: OK");
        
        System.out.println("   Query Methods: PASSED");
    }
}