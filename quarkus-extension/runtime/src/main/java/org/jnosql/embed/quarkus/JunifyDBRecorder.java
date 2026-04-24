package org.jnosql.embed.quarkus;

import io.quarkus.arc.runtime.BeanContainer;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;
import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.kv.KeyValueBucket;
import org.junify.db.storage.spi.H2StorageEngine;

@Recorder
public class JunifyDBRecorder {

    public RuntimeValue<JunifyDB> createDatabase(JunifyDBQuarkusConfig config) {
        var builder = JunifyDB.embed()
            .storageEngine(config.getEngine())
            .persistTo(config.getDataDir())
            .autoFlush(config.isAutoFlush())
            .flushIntervalMs(config.getFlushIntervalMs());

        var db = JunifyDB.create(builder.buildConfig());
        
        return new RuntimeValue<>(db);
    }

    public RuntimeValue<H2StorageEngine> createH2Engine(JunifyDBQuarkusConfig config) {
        var h2Engine = new H2StorageEngine(
            java.nio.file.Paths.get(config.getDataDir(), "h2"),
            "junifydb"
        );
        
        return new RuntimeValue<>(h2Engine);
    }

    public void startServer(RuntimeValue<JunifyDB> dbValue, JunifyDBQuarkusConfig config) {
        if (config.isEnableServer()) {
            try {
                var db = dbValue.getValue();
                var server = db.startServer(config.getPort());
                
                if (config.getApiKey().isPresent()) {
                    server.setApiKey(config.getApiKey().get());
                }
                
                System.out.println("JunifyDB server started on port " + config.getPort());
            } catch (Exception e) {
                throw new RuntimeException("Failed to start JunifyDB server", e);
            }
        }
    }

    public void stopServer(RuntimeValue<JunifyDB> dbValue) {
        try {
            var db = dbValue.getValue();
            if (db != null && db.isOpen()) {
                db.close();
            }
        } catch (Exception e) {
            System.err.println("Error closing JunifyDB: " + e.getMessage());
        }
    }
}