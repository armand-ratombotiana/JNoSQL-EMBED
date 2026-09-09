package org.junify.db.demo.micronaut;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;
import org.junify.db.JunifyDB;
import org.junify.db.demo.model.Product;
import org.junify.db.demo.model.SampleData;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.document.Query;
import org.junify.db.nosql.kv.KeyValueBucket;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Controller("/api/products")
public class ProductController {

    private final DocumentCollection productCollection;
    private final KeyValueBucket priceCache;

    @Inject
    public ProductController(JunifyDB junifyDB) {
        this.productCollection = junifyDB.documentCollection("products");
        this.priceCache = junifyDB.keyValueBucket("price_cache");
        seedSampleDataIfEmpty();
    }

    private void seedSampleDataIfEmpty() {
        if (productCollection.count() == 0) {
            for (Product p : SampleData.products()) {
                productCollection.insert(p.toDocument());
                priceCache.put(p.id(), String.valueOf(p.price()));
            }
        }
    }

    @Get
    public List<Product> getAllProducts() {
        return productCollection.findAll().stream()
                .map(Product::fromDocument)
                .collect(Collectors.toList());
    }

    @Get("/{id}")
    public HttpResponse<Product> getProductById(@PathVariable String id) {
        Document doc = productCollection.findById(id);
        if (doc == null) {
            return HttpResponse.notFound();
        }
        return HttpResponse.ok(Product.fromDocument(doc));
    }

    @Post
    public HttpResponse<Product> createProduct(@Body Product product) {
        String productId = product.id() != null && !product.id().isBlank()
                ? product.id()
                : "prod-" + System.currentTimeMillis();

        Product toSave = new Product(
                productId,
                product.sku(),
                product.name(),
                product.category(),
                product.price(),
                product.tags(),
                product.attributes()
        );

        productCollection.insert(toSave.toDocument());
        priceCache.put(toSave.id(), String.valueOf(toSave.price()));
        return HttpResponse.created(toSave);
    }

    @Get("/category/{category}")
    public List<Product> getProductsByCategory(@PathVariable String category) {
        return productCollection.find(Query.eq("category", category)).stream()
                .map(Product::fromDocument)
                .collect(Collectors.toList());
    }

    @Get("/{id}/cached-price")
    public HttpResponse<Map<String, Object>> getCachedPrice(@PathVariable String id) {
        String price = priceCache.get(id);
        if (price == null) {
            return HttpResponse.notFound();
        }
        return HttpResponse.ok(Map.of("id", id, "cachedPrice", Double.parseDouble(price)));
    }
}
