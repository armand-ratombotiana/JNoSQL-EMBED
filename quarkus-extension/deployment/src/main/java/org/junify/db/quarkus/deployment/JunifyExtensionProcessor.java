package org.junify.db.quarkus.deployment;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import org.junify.db.quarkus.JunifyConfig;
import org.junify.db.quarkus.JunifyDBProducer;
import org.junify.db.quarkus.JunifyRecorder;

import java.util.List;

/**
 * Quarkus build-time extension processor for JunifyDB.
 *
 * <p>Registers {@link JunifyDBProducer} as an unremovable CDI bean, records the
 * database initialization for the runtime-init phase, and configures native image resources.
 */
class JunifyExtensionProcessor {

    private static final String FEATURE = "junifydb-embed";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    AdditionalBeanBuildItem registerBeans() {
        return AdditionalBeanBuildItem.unremovableOf(JunifyDBProducer.class);
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void initialize(JunifyRecorder recorder, JunifyConfig config) {
        recorder.createDatabase(config);
    }

    @BuildStep
    NativeImageResourceBuildItem nativeResources() {
        return new NativeImageResourceBuildItem(List.of(
                "org/junify/db/core/util/JsonSerde.class"
        ));
    }
}
