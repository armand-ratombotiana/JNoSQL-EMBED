package org.jnosql.embed.quarkus;

import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;
import org.junify.db.JunifyDB;

/**
 * Quarkus recorder for JunifyDB.
 * Creates and manages the {@link JunifyDB} instance at runtime init.
 */
@Recorder
public class JunifyDBRecorder {

    public RuntimeValue<JunifyDB> createDatabase(JunifyDBQuarkusConfig config) {
        var db = JunifyDB.create(
                JunifyDB.embed()
                        .storageEngine(config.getEngine())
                        .persistTo(config.getDataDir())
                        .autoFlush(config.isAutoFlush())
                        .flushIntervalMs(config.getFlushIntervalMs())
                        .buildConfig()
        );
        return new RuntimeValue<>(db);
    }

    public void stopDatabase(RuntimeValue<JunifyDB> dbValue) {
        try {
            var db = dbValue.getValue();
            if (db != null && db.isOpen()) {
                db.close();
            }
        } catch (Exception e) {
            System.err.println("[JunifyDB] Error closing database: " + e.getMessage());
        }
    }
}