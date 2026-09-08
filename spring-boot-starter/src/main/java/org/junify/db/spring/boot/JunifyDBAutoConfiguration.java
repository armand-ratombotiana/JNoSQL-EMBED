package org.junify.db.spring.boot;

import org.junify.db.JunifyDB;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(JunifyDBProperties.class)
@ConditionalOnProperty(prefix = "junifydb", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JunifyDBAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public JunifyDB junifyDB(JunifyDBProperties properties) {
        var builder = JunifyDB.embed()
                .storageEngine(properties.getStorageEngine())
                .autoFlush(properties.isAutoFlush())
                .flushIntervalMs(properties.getFlushIntervalMs());

        if (properties.getDataDir() != null && !properties.getDataDir().isBlank()) {
            builder.persistTo(properties.getDataDir());
        }

        return JunifyDB.create(builder.buildConfig());
    }

    @Bean
    @ConditionalOnMissingBean
    public JunifyDBTemplate junifyDBTemplate(JunifyDB junifyDB) {
        return new JunifyDBTemplate(junifyDB);
    }
}
