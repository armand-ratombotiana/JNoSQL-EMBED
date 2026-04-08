package org.jnosql.embed.quarkus;

import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;
import org.jnosql.embed.JNoSQL;
import org.jnosql.embed.config.JNoSQLConfig;

@Recorder
public class JNoSQLRecorder {

    public RuntimeValue<JNoSQL> createJNoSQL(JNoSQLConfig config) {
        var builder = JNoSQL.embed()
                .storageEngine(config.getStorageEngine())
                .persistTo(config.getDataDir())
                .autoFlush(config.isAutoFlush());
        
        return new RuntimeValue<>(builder.build());
    }
}
