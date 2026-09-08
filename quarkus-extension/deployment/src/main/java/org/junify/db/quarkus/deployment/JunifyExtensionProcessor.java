package org.junify.db.quarkus.deployment;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import org.junify.db.quarkus.JunifyConfig;
import org.junify.db.quarkus.JunifyRecorder;

/**
 * Quarkus build-time processor for the {@code junify.*} extension.
 */
class JunifyExtensionProcessor {

    private static final String FEATURE = "junify-embed";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void initialize(JunifyRecorder recorder, JunifyConfig config) {
        recorder.createDatabase(config);
    }
}
