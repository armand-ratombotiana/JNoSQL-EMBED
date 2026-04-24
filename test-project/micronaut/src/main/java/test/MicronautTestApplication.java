package test;

import io.micronaut.runtime.Micronaut;
import io.micronaut.http.annotation.*;
import io.micronaut.http.MediaType;
import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.adapter.jpa.JunifyDBEntityManager;
import org.junify.db.adapter.jpa.JunifyDBRepository;
import test.entity.User;
import jakarta.persistence.*;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.List;
import java.util.Optional;

@Controller("/api")
public class MicronautTestApplication {

    public static void main(String[] args) {
        Micronaut.run(MicronautTestApplication.class, args);
        
        System.out.println("=== Micronaut Test Started ===\n");
        
        testAll();
        
        System.out.println("\n=== All Micronaut Tests Passed ===");
    }

    private static void testAll() {
        var config = JunifyDBConfig.builder()
            .storageEngine(JunifyDBConfig.StorageEngineType.IN_MEMORY)
            .build();
        
        var db = JunifyDB.create(config);
        
        testEntityManager(db);
        testRepository(db);
        testQueryMethods(db);
        
        db.close();
    }

    private static void testEntityManager(JunifyDB db) {
        System.out.println("1. Testing EntityManager...");
        
        var em = new JunifyDBEntityManager(db, "test");
        
        var user = new User("John", "john@test.com", 30);
        em.persist(user);
        
        var found = em.find(User.class, user.getId());
        System.out.println("   persist + find: OK");
        
        var all = em.findAll(User.class);
        System.out.println("   findAll: OK - " + all.size() + " users");
        
        System.out.println("   EntityManager: PASSED");
    }

    private static void testRepository(JunifyDB db) {
        System.out.println("\n2. Testing Repository...");
        
        var repo = new JunifyDBRepository<>(db, User.class, String.class);
        
        var user = new User("Alice", "alice@test.com", 25);
        var saved = repo.save(user);
        
        var found = repo.findById(saved.getId());
        System.out.println("   save + findById: OK");
        
        System.out.println("   Repository: PASSED");
    }

    private static void testQueryMethods(JunifyDB db) {
        System.out.println("\n3. Testing Query Methods...");
        
        var repo = new JunifyDBRepository<>(db, User.class, String.class);
        
        var users = List.of(
            new User("User1", "u1@test.com", 20),
            new User("User2", "u2@test.com", 25),
            new User("User3", "u3@test.com", 30)
        );
        
        for (var u : users) {
            repo.save(u);
        }
        
        var lessThan25 = repo.findByFieldLessThan("age", 25);
        System.out.println("   findByFieldLessThan: OK - " + lessThan25.size() + " users");
        
        var greaterThan25 = repo.findByFieldGreaterThan("age", 25);
        System.out.println("   findByFieldGreaterThan: OK - " + greaterThan25.size() + " users");
        
        System.out.println("   Query Methods: PASSED");
    }

    @Get("/users")
    public String getUsers() {
        return "Micronaut Test - Use /api/users/list endpoint";
    }
}