package org.jnosql.embed.quarkus;

import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;
import org.junify.db.JunifyDB;

/**
 * Quarkus recorder for the legacy {@code jnosql.*} configuration prefix.
 * Delegates to {@link JunifyDB} using the {@link JNoSQLConfig} settings.
 */
@Recorder
public class JNoSQLRecorder {

    public RuntimeValue<JunifyDB> createJNoSQL(JNoSQLConfig config) {
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
