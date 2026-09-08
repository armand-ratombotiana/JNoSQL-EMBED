package org.jnosql.embed.quarkus.deployment;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import org.jnosql.embed.quarkus.JNoSQLConfig;
import org.jnosql.embed.quarkus.JNoSQLRecorder;

/**
 * Quarkus build-time processor for the legacy {@code jnosql.*} prefix.
 */
class JNoSQLExtensionProcessor {

    private static final String FEATURE = "jnosql-embed";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void initialize(JNoSQLRecorder recorder, JNoSQLConfig config) {
        recorder.createJNoSQL(config);
    }
}
