package org.junify.db.demo.spring;

import org.junify.db.JunifyDB;
import org.junify.db.demo.model.Order;
import org.junify.db.demo.model.OrderItem;
import org.junify.db.nosql.document.Document;
import org.junify.db.transaction.mvcc.Transaction;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class OrderService {

    private final JunifyDB db;

    public OrderService(JunifyDB db) {
        this.db = db;
    }

    public Order placeOrder(Order order) {
        String orderId = order.id() != null ? order.id() : "ord-" + UUID.randomUUID().toString().substring(0, 8);
        String orderNumber = "ORD-" + System.currentTimeMillis();

        double total = order.items().stream().mapToDouble(OrderItem::subtotal).sum();
        Order finalized = new Order(
                orderId,
                orderNumber,
                order.customerId(),
                order.items(),
                total,
                "CONFIRMED",
                System.currentTimeMillis()
        );

        // Transactional commit across orders and audit log
        try (Transaction tx = db.beginTransaction()) {
            tx.write("orders", finalized.id(), finalized.toDocument().toJson());
            tx.write("audit_events", "audit-" + System.currentTimeMillis(),
                    "{\"action\":\"ORDER_PLACED\",\"orderId\":\"" + finalized.id() + "\",\"total\":" + total + "}");
            tx.commit();
        }

        return finalized;
    }

    public Order getOrder(String id) {
        Document doc = db.documentCollection("orders").findById(id);
        return Order.fromDocument(doc);
    }
}
