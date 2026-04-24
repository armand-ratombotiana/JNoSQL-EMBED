package test;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import jakarta.nosql.Id;
import java.io.Serializable;

@Entity
@Table(name = "quarkus_users")
public class User extends PanacheEntity {

    @Column(nullable = false, length = 100)
    public String name;

    @Column(unique = true)
    public String email;

    @Column(nullable = false)
    public Integer age;

    public boolean active;

    public User() {
    }

    public User(String name, String email, Integer age) {
        this.name = name;
        this.email = email;
        this.age = age;
        this.active = true;
    }

    public static User findByEmail(String email) {
        return find("email", email).firstResult();
    }

    public static java.util.List<User> findByAgeGreaterThan(int minAge) {
        return find("age > ?1", minAge).list();
    }
}