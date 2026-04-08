package org.jnosql.embed.quarkus.deployment;

import io.quarkus.arc.deployment.BeanContainerListenerBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import org.jnosql.embed.quarkus.JNoSQLConfig;
import org.jnosql.embed.quarkus.JNoSQLRecorder;

import java.util.Arrays;

class JNoSQLExtensionProcessor {

    private static final String FEATURE = "jnosql-embed";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    BeanContainerListenerBuildItem container(
            JNoSQLRecorder recorder,
            JNoSQLConfig config) {
        return new BeanContainerListenerBuildItem(
                recorder.createJNoSQL(config)
        );
    }
}
