package org.junify.db.quarkus;

import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;
import org.junify.db.JunifyDB;

/**
 * Quarkus runtime recorder for JunifyDB.
 * Manages database initialization and clean shutdown lifecycle during runtime init.
 */
@Recorder
public class JunifyRecorder {

    public RuntimeValue<JunifyDB> createDatabase(JunifyConfig config) {
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
