package org.junify.db.spring.boot;

import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.storage.spi.H2StorageEngine;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
@EnableConfigurationProperties(JunifyDBProperties.class)
@ConditionalOnProperty(prefix = "junifydb", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JunifyDBAutoConfiguration {

    @Bean
    @Primary
    @ConditionalOnMissingBean(name = "junifyDB")
    public JunifyDB junifyDB(JunifyDBProperties properties) {
        
        var configBuilder = JunifyDB.embed()
            .storageEngine(properties.getStorageEngine())
            .autoFlush(properties.isAutoFlush())
            .flushIntervalMs(properties.getFlushIntervalMs());
        
        if (properties.getDataDir() != null && !properties.getDataDir().isEmpty()) {
            configBuilder.persistTo(properties.getDataDir());
        }
        
        return JunifyDB.create(configBuilder.buildConfig());
    }

    @Bean
    @Primary
    @ConditionalOnMissingBean(name = "h2StorageEngine")
    public H2StorageEngine h2StorageEngine(JunifyDBProperties properties) {
        if (!properties.getStorageEngine().name().equals("H2")) {
            var config = JunifyDB.embed()
                .storageEngine(org.junify.db.config.JunifyDBConfig.StorageEngineType.H2)
                .buildConfig();
            return new H2StorageEngine(
                Paths.get(properties.getDataDir(), "h2db"),
                "embeddb"
            );
        }
        return null;
    }

    @Bean
    public JunifyDBPropertiesCustomizer junifyDBPropertiesCustomizer(JunifyDBProperties properties) {
        return new JunifyDBPropertiesCustomizer(properties);
    }

    public static class JunifyDBPropertiesCustomizer {
        private final JunifyDBProperties properties;

        public JunifyDBPropertiesCustomizer(JunifyDBProperties properties) {
            this.properties = properties;
        }

        public void customize(JunifyDB db) {
            if (properties.getMaxPoolSize() != null) {
                // Apply pool size settings
            }
            if (properties.getApiKey() != null && !properties.getApiKey().isEmpty()) {
                try {
                    var server = db.startServer(properties.getPort());
                    server.setApiKey(properties.getApiKey());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to start server", e);
                }
            }
        }
    }
}