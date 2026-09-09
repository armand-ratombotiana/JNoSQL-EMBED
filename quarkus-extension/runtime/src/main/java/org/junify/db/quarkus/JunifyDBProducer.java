package org.junify.db.quarkus;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.junify.db.JunifyDB;
import org.junify.db.core.event.EventBus;
import org.junify.db.core.metrics.DatabaseMetrics;
import org.junify.db.nosql.column.ColumnFamily;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.kv.HashBucket;
import org.junify.db.nosql.kv.KeyValueBucket;
import org.junify.db.nosql.kv.ListBucket;
import org.junify.db.nosql.kv.SetBucket;

/**
 * CDI producer for JunifyDB beans in a Quarkus application.
 *
 * <p>Produces a singleton {@link JunifyDB} instance configured from
 * {@link JunifyConfig}, plus convenience beans for document collections,
 * key-value buckets, Redis-style data structures, column families, event bus,
 * and database metrics.
 *
 * <p>All beans are marked {@code @DefaultBean} so applications can override
 * them with their own {@code @Produces} methods.
 */
@ApplicationScoped
public class JunifyDBProducer {

    @Inject
    JunifyConfig config;

    @Produces
    @Singleton
    @DefaultBean
    public JunifyDB createDatabase() {
        return JunifyDB.create(
                JunifyDB.embed()
                        .storageEngine(config.getEngine())
                        .persistTo(config.getDataDir())
                        .autoFlush(config.isAutoFlush())
                        .flushIntervalMs(config.getFlushIntervalMs())
                        .buildConfig()
        );
    }

    @Produces
    @ApplicationScoped
    @DefaultBean
    public DocumentCollection defaultDocumentCollection(JunifyDB db) {
        return db.documentCollection("default");
    }

    @Produces
    @ApplicationScoped
    @DefaultBean
    public KeyValueBucket defaultKeyValueBucket(JunifyDB db) {
        return db.keyValueBucket("default");
    }

    @Produces
    @ApplicationScoped
    @DefaultBean
    public ListBucket defaultListBucket(JunifyDB db) {
        return db.listBucket("default");
    }

    @Produces
    @ApplicationScoped
    @DefaultBean
    public SetBucket defaultSetBucket(JunifyDB db) {
        return db.setBucket("default");
    }

    @Produces
    @ApplicationScoped
    @DefaultBean
    public HashBucket defaultHashBucket(JunifyDB db) {
        return db.hashBucket("default");
    }

    @Produces
    @ApplicationScoped
    @DefaultBean
    public ColumnFamily defaultColumnFamily(JunifyDB db) {
        return db.columnFamily("default");
    }

    @Produces
    @ApplicationScoped
    @DefaultBean
    public EventBus eventBus(JunifyDB db) {
        return db.eventBus();
    }

    @Produces
    @ApplicationScoped
    @DefaultBean
    public DatabaseMetrics databaseMetrics(JunifyDB db) {
        return db.metrics();
    }
}
