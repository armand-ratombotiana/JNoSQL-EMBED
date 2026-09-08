package org.jnosql.embed.quarkus;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.kv.KeyValueBucket;

/**
 * CDI producer for JunifyDB beans in a Quarkus application.
 *
 * <p>Produces a singleton {@link JunifyDB} instance configured from
 * {@link JunifyDBQuarkusConfig}, plus convenience {@link DocumentCollection}
 * and {@link KeyValueBucket} beans for the "default" namespace.
 *
 * <p>All beans are marked {@code @DefaultBean} so applications can override
 * them with their own {@code @Produces} methods.
 */
@ApplicationScoped
public class JunifyDBProducer {

    @Inject
    JunifyDBQuarkusConfig config;

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
}