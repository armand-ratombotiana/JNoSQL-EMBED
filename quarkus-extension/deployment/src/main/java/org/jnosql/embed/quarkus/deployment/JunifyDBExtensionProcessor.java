package org.jnosql.embed.quarkus.deployment;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import org.jnosql.embed.quarkus.JunifyDBProducer;
import org.jnosql.embed.quarkus.JunifyDBQuarkusConfig;
import org.jnosql.embed.quarkus.JunifyDBRecorder;

import java.util.List;

/**
 * Quarkus build-time extension processor for JunifyDB.
 *
 * <p>Registers {@link JunifyDBProducer} as a non-removable CDI bean and
 * records the database initialization for the runtime-init phase.
 */
class JunifyDBExtensionProcessor {

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
    void initialize(JunifyDBRecorder recorder, JunifyDBQuarkusConfig config) {
        recorder.createDatabase(config);
    }


    @BuildStep
    NativeImageResourceBuildItem nativeResources() {
        return new NativeImageResourceBuildItem(List.of(
                "org/junify/db/core/util/JsonSerde.class"
        ));
    }
}