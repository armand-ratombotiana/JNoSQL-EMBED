package org.jnosql.embed.quarkus.deployment;

import io.quarkus.arc.deployment.BeanContainerListenerBuildItem;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import org.jnosql.embed.quarkus.JNoSQLRecorder;
import org.jnosql.embed.quarkus.JunifyDBQuarkusConfig;
import org.jnosql.embed.quarkus.JunifyDBProducer;

import java.util.Arrays;

class JunifyDBExtensionProcessor {

    private static final String FEATURE = "junifydb-embed";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    AdditionalBeanBuildItem beans() {
        return AdditionalBeanBuildItem.unremovableOf(JunifyDBProducer.class);
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    BeanContainerListenerBuildItem initialize(
            JunifyDBRecorder recorder,
            JunifyDBQuarkusConfig config) {
        
        recorder.createDatabase(config);
        
        if (config.isEnableServer()) {
            recorder.startServer(null, config);
        }
        
        return new BeanContainerListenerBuildItem(
            container -> {
                // Container is ready
            }
        );
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_STOP)
    void shutdown(JunifyDBRecorder recorder) {
        recorder.stopServer(null);
    }

    @BuildStep
    NativeImageResourceBuildItem nativeResources() {
        return new NativeImageResourceBuildItem(
            Arrays.asList(
                "org.junify.db.core.util.JsonSerde",
                "org.junify.db.storage.spi.H2StorageEngine"
            )
        );
    }
}