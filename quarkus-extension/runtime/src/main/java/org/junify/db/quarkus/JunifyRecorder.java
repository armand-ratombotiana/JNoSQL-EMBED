package org.junify.db.quarkus;

import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;
import org.junify.db.Junify;
import org.junify.db.config.JunifyConfig;

@Recorder
public class JunifyRecorder {

    public RuntimeValue<Junify> createJunify(JunifyConfig config) {
        var builder = Junify.embed()
                .storageEngine(config.getStorageEngine())
                .persistTo(config.getDataDir())
                .autoFlush(config.isAutoFlush());
        
        return new RuntimeValue<>(builder.build());
    }
}
