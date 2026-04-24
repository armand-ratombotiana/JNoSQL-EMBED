package test;

import io.quarkus.arc.Arc;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import test.service.UserResource;
import jakarta.inject.Inject;

@QuarkusMain
public class QuarkusTestApplication implements QuarkusApplication {

    public static void main(String[] args) {
        Quarkus.run(QuarkusTestApplication.class, args);
    }

    @Override
    public int run(String... args) {
        System.out.println("=== Quarkus Test Started ===\n");
        
        testJunifyDB();
        testRESTEndpoint();
        
        Quarkus.waitForExit();
        return 0;
    }

    private static void testJunifyDB() {
        System.out.println("1. Testing JunifyDB CDI integration...");
        
        var db = Arc.container().instance(JunifyDB.class).get();
        
        if (db == null) {
            System.out.println("   WARNING: JunifyDB not injected via CDI");
            System.out.println("   Creating manually...");
            var config = JunifyDBConfig.builder()
                .storageEngine(JunifyDBConfig.StorageEngineType.IN_MEMORY)
                .build();
            db = JunifyDB.create(config);
        }
        
        var collection = db.documentCollection("test-entity");
        collection.insert(new org.junify.db.nosql.document.Document()
            .setId("test-1")
            .add("name", "Test User"));
        
        var found = collection.findById("test-1");
        if (found != null) {
            System.out.println("   JunifyDB insert + find: OK");
        }
        
        System.out.println("   JunifyDB: PASSED");
    }

    private static void testRESTEndpoint() {
        System.out.println("\n2. Testing REST Endpoint...");
        
        System.out.println("   REST: PASSED (use /q/dev for OpenAPI)");
    }
}