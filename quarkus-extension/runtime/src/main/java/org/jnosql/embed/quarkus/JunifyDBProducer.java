package org.jnosql.embed.quarkus;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.kv.KeyValueBucket;
import org.junify.db.storage.spi.H2StorageEngine;

@ApplicationScoped
public class JunifyDBProducer {

    @Inject
    JunifyDBQuarkusConfig config;

    @Produces
    @Singleton
    @DefaultBean
    public JunifyDB createDatabase() {
        var builder = JunifyDB.embed()
            .storageEngine(config.getEngine())
            .persistTo(config.getDataDir())
            .autoFlush(config.isAutoFlush())
            .flushIntervalMs(config.getFlushIntervalMs());

        return JunifyDB.create(builder.buildConfig());
    }

    @Produces
    @ApplicationScoped
    public DocumentCollection documentCollection(JunifyDB db) {
        return db.documentCollection("default");
    }

    @Produces
    @ApplicationScoped
    public KeyValueBucket keyValueBucket(JunifyDB db) {
        return db.keyValueBucket("default");
    }

    @Produces
    @Singleton
    public H2StorageEngine h2StorageEngine(JunifyDB db) {
        if (db.h2Engine() != null) {
            return db.h2Engine();
        }
        return null;
    }
}