package org.junify.db.quarkus;

import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;
import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.quarkus.JunifyConfig;

/**
 * Quarkus recorder for the {@code junify.*} config prefix.
 * Creates the {@link JunifyDB} instance during runtime initialisation.
 */
@Recorder
public class JunifyRecorder {

    public RuntimeValue<JunifyDB> createDatabase(JunifyConfig config) {
        var db = JunifyDB.create(
                JunifyDB.embed()
                        .storageEngine(config.getStorageEngine())
                        .persistTo(config.getDataDir())
                        .autoFlush(config.isAutoFlush())
                        .buildConfig()
        );
        return new RuntimeValue<>(db);
    }
}
