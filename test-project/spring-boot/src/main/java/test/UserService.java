package test;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.junify.db.adapter.jpa.JunifyDBRepository;
import org.junify.db.adapter.jpa.JunifyDBEntityManager;
import org.junify.db.adapter.jnosql.JunifyDBTemplate;
import test.entity.User;
import jakarta.persistence.*;
import jakarta.nosql.*;
import java.util.List;
import java.util.Optional;

@Service
@Transactional
public class UserService {

    @PersistenceContext
    private EntityManager em;

    @PersistenceUnit
    private EntityManagerFactory emf;

    @Autowired
    private JunifyDBTemplate template;

    @Autowired
    private JunifyDBRepository<User, String> userRepository;

    public void testAll() {
        testEntityManager();
        testRepository();
        testTemplate();
    }

    private void testEntityManager() {
        System.out.println("1. Testing EntityManager (JPA)...");
        
        var user = new User("John", "john@test.com", 30);
        em.persist(user);
        
        var found = em.find(User.class, user.getId());
        if (found == null) {
            throw new RuntimeException("persist/find failed");
        }
        
        System.out.println("   persist + find: OK");
        
        var query = em.createQuery("SELECT u FROM User u", User.class);
        var results = query.getResultList();
        
        System.out.println("   JPQL Query: OK");
        
        System.out.println("   EntityManager: PASSED");
    }

    private void testRepository() {
        System.out.println("\n2. Testing Repository...");
        
        var user = new User("Alice", "alice@test.com", 25);
        var saved = userRepository.save(user);
        
        var found = userRepository.findById(saved.getId());
        if (!found.isPresent()) {
            throw new RuntimeException("save/findById failed");
        }
        
        System.out.println("   save + findById: OK");
        
        var all = userRepository.findAll();
        System.out.println("   findAll: OK - " + all.size() + " users");
        
        var byName = userRepository.findByField("name", "Alice");
        System.out.println("   findByField: OK - " + byName.size() + " users");
        
        System.out.println("   Repository: PASSED");
    }

    private void testTemplate() {
        System.out.println("\n3. Testing Template (Jakarta NoSQL)...");
        
        var user = new User("Bob", "bob@test.com", 35);
        
        template.insert(user);
        
        var found = template.find(User.class, user.getId());
        if (!found.isPresent()) {
            throw new RuntimeException("insert/find failed");
        }
        
        System.out.println("   insert + find: OK");
        
        System.out.println("   Template: PASSED");
    }
}