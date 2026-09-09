package org.junify.db.demo.micronaut;

import io.micronaut.serde.annotation.SerdeImport;
import org.junify.db.demo.model.Customer;
import org.junify.db.demo.model.InventoryItem;
import org.junify.db.demo.model.Order;
import org.junify.db.demo.model.OrderItem;
import org.junify.db.demo.model.Product;

@SerdeImport(Product.class)
@SerdeImport(Order.class)
@SerdeImport(OrderItem.class)
@SerdeImport(Customer.class)
@SerdeImport(InventoryItem.class)
public class SerdeConfiguration {
}
