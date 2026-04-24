package test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.nosql.repository.config.EnableNoSqlRepositories;

@SpringBootApplication
@EntityScan(basePackages = "test")
@EnableJpaRepositories(basePackages = "test")
@EnableNoSqlRepositories(basePackages = "test")
public class SpringBootTestApplication {

    public static void main(String[] args) {
        var context = SpringApplication.run(SpringBootTestApplication.class, args);
        
        System.out.println("=== Spring Boot Test Started ===\n");
        
        var userService = context.getBean(UserService.class);
        userService.testAll();
        
        context.close();
        
        System.out.println("\n=== All Spring Boot Tests Passed ===");
    }
}