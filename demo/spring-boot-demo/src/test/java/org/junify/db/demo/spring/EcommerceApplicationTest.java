package org.junify.db.demo.spring;

import org.junify.db.JunifyDB;
import org.junify.db.demo.model.Order;
import org.junify.db.demo.model.OrderItem;
import org.junify.db.demo.model.Product;
import org.junify.db.spring.boot.JunifyDBTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class EcommerceApplicationTest {

    @Autowired
    private JunifyDB db;

    @Autowired
    private JunifyDBTemplate template;

    @Autowired
    private ProductService productService;

    @Autowired
    private OrderService orderService;

    @Test
    void testContextAndBeansInjected() {
        assertNotNull(db, "JunifyDB bean should be injected");
        assertNotNull(template, "JunifyDBTemplate bean should be injected");
        assertTrue(db.isOpen(), "Database should be open");
    }

    @Test
    void testProductCrudAndQuery() {
        Product book = new Product(
                "prod-test-01",
                "BOOK-01",
                "Java Architecture Guide",
                "Books",
                45.00,
                List.of("java", "architecture"),
                Map.of("edition", 3)
        );

        productService.save(book);

        Optional<Product> fetched = productService.findById("prod-test-01");
        assertTrue(fetched.isPresent(), "Product should be found by ID");
        assertEquals("Java Architecture Guide", fetched.get().name());
        assertEquals("Books", fetched.get().category());

        List<Product> books = productService.findByCategory("Books");
        assertFalse(books.isEmpty(), "Category search should find books");
        assertTrue(books.stream().anyMatch(p -> "BOOK-01".equals(p.sku())));
    }

    @Test
    void testTransactionalOrderPlacement() {
        Order order = new Order(
                "ord-test-99",
                "ORD-99",
                "cust-001",
                List.of(
                        new OrderItem("prod-101", "Developer Ultrabook", 1, 1899.99),
                        new OrderItem("prod-102", "Mechanical Keyboard", 2, 249.50)
                ),
                0.0,
                "NEW",
                System.currentTimeMillis()
        );

        Order placed = orderService.placeOrder(order);
        assertNotNull(placed);
        assertEquals(2398.99, placed.totalAmount(), 0.01);
        assertEquals("CONFIRMED", placed.status());

        Order reloaded = orderService.getOrder("ord-test-99");
        assertNotNull(reloaded, "Order should be persisted in orders collection");
        assertEquals("cust-001", reloaded.customerId());
        assertEquals(2, reloaded.items().size());
    }
}
