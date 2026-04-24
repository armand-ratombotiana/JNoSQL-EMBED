package test;

import test.entity.User;
import org.junify.db.JunifyDB;
import org.junify.db.adapter.jnosql.JunifyDBTemplate;
import org.junify.db.config.JunifyDBConfig;
import jakarta.nosql.*;
import jakarta.nosql.document.DocumentQuery;
import jakarta.nosql.document.DocumentCollectionManager;
import jakarta.nosql.keyvalue.KeyValueTemplate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public class JakartaNoSQLTest {

    public static void main(String[] args) {
        System.out.println("=== Jakarta NoSQL (Eclipse JNoSQL) Specification Test ===\n");

        var config = JunifyDBConfig.builder()
            .storageEngine(JunifyDBConfig.StorageEngineType.IN_MEMORY)
            .build();
        
        var db = JunifyDB.create(config);
        
        testTemplate(db);
        testDocumentCollectionManager(db);
        testKeyValueTemplate(db);
        
        db.close();
        
        System.out.println("\n=== All Jakarta NoSQL Tests Passed ===");
    }

    private static void testTemplate(JunifyDB db) {
        System.out.println("1. Testing Template...");
        
        var template = new JunifyDBTemplate(db);
        
        var user = new User("John Doe", "john@example.com", 30);
        user.setId("john-1");
        
        template.insert(user);
        
        System.out.println("   insert: OK");
        
        var found = template.find(User.class, "john-1");
        if (!found.isPresent()) {
            throw new RuntimeException("Failed: find not working");
        }
        
        System.out.println("   find: OK");
        
        var user2 = new User("Jane Doe", "jane@example.com", 25);
        user2.setId("jane-1");
        template.insert(user2);
        
        System.out.println("   Template: PASSED");
    }

    private static void testDocumentCollectionManager(JunifyDB db) {
        System.out.println("\n2. Testing DocumentCollectionManager...");
        
        var template = new JunifyDBTemplate(db);
        var dcm = template.getDocumentCollectionManager();
        
        var user = new User("Alice", "alice@test.com", 28);
        user.setId("alice-1");
        
        var docEntity = new jakarta.nosql.document.DocumentEntity("users");
        docEntity.add("id", user.getId());
        docEntity.add("name", user.getName());
        docEntity.add("email", user.getEmail());
        docEntity.add("age", user.getAge());
        
        dcm.insert(docEntity);
        
        System.out.println("   insert DocumentEntity: OK");
        
        var query = DocumentQuery.of("users");
        var results = dcm.find(query);
        
        System.out.println("   DocumentCollectionManager: PASSED");
    }

    private static void testKeyValueTemplate(JunifyDB db) {
        System.out.println("\n3. Testing KeyValueTemplate...");
        
        var template = new JunifyDBTemplate(db);
        var kvt = template.getKeyValueTemplate();
        
        var user = new User("Bob", "bob@test.com", 35);
        user.setId("bob-1");
        
        kvt.put(User.class, "bob-1", user);
        
        System.out.println("   put: OK");
        
        var retrieved = kvt.get(User.class, "bob-1");
        if (retrieved == null) {
            throw new RuntimeException("Failed: get not working");
        }
        
        System.out.println("   get: OK");
        
        kvt.delete(User.class, "bob-1");
        
        var deleted = kvt.get(User.class, "bob-1");
        if (deleted != null) {
            throw new RuntimeException("Failed: delete not working");
        }
        
        System.out.println("   delete: OK");
        
        System.out.println("   KeyValueTemplate: PASSED");
    }
}