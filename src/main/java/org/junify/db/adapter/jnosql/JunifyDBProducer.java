package org.junify.db.adapter.jnosql;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.transaction.mvcc.MVCCManager;

/**
 * CDI Producer for core JunifyDB components.
 * Produces JunifyDB, MVCCManager, and adapter beans for injection.
 */
public class JunifyDBProducer {

    private JunifyDB db;

    @Produces
    @ApplicationScoped
    public JunifyDB produceJunifyDB() {
        if (db == null) {
            db = JunifyDBConfig.builder()
                    .storageEngine(JunifyDBConfig.StorageEngineType.IN_MEMORY)
                    .autoFlush(true)
                    .build();
        }
        return db;
    }

    public void close(@Disposes JunifyDB db) {
        if (db.isOpen()) db.close();
    }

    @Produces
    @ApplicationScoped
    public MVCCManager produceMVCC(JunifyDB db) {
        return db.mvcc();
    }

    @Produces
    @ApplicationScoped
    public DocumentTemplate produceDocumentTemplate(JunifyDB db) {
        return EclipseDocumentTemplate.of(db.documentCollection("_default"));
    }

    @Produces
    @ApplicationScoped
    public KeyValueTemplate produceKeyValueTemplate(JunifyDB db) {
        return KeyValueTemplate.of(db.keyValueBucket("_default"));
    }
}
