package org.junify.db.spring;

import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(JunifyDBProperties.class)
public class JunifyDBAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public JunifyDB JunifyDB(JunifyDBProperties properties) {
        return JUNIFYDB.embed()
                .storageEngine(properties.getStorageEngine())
                .persistTo(properties.getDataDir())
                .autoFlush(properties.isAutoFlush())
                .flushIntervalMs(properties.getFlushIntervalMs())
                .build();
    }
}
