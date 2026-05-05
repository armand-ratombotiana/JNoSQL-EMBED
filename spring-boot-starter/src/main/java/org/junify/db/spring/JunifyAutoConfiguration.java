package org.junify.db.spring;

import org.junify.db.Junify;
import org.junify.db.config.JunifyConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(JunifyProperties.class)
public class JunifyAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Junify Junify(JunifyProperties properties) {
        return Junify.embed()
                .storageEngine(properties.getStorageEngine())
                .persistTo(properties.getDataDir())
                .autoFlush(properties.isAutoFlush())
                .flushIntervalMs(properties.getFlushIntervalMs())
                .build();
    }
}
