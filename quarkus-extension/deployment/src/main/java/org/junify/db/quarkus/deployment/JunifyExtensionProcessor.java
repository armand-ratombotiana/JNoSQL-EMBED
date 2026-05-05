package org.junify.db.quarkus.deployment;

import io.quarkus.arc.deployment.BeanContainerListenerBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import org.junify.db.quarkus.JunifyConfig;
import org.junify.db.quarkus.JunifyRecorder;

import java.util.Arrays;

class JunifyExtensionProcessor {

    private static final String FEATURE = "Junify-embed";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    BeanContainerListenerBuildItem container(
            JunifyRecorder recorder,
            JunifyConfig config) {
        return new BeanContainerListenerBuildItem(
                recorder.createJunify(config)
        );
    }
}
