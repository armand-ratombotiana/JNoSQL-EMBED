package org.jnosql.embed.spring;

import org.jnosql.embed.JNoSQL;
import org.jnosql.embed.config.JNoSQLConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(JNoSQLProperties.class)
public class JNoSQLAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public JNoSQL jnosql(JNoSQLProperties properties) {
        return JNoSQL.embed()
                .storageEngine(properties.getStorageEngine())
                .persistTo(properties.getDataDir())
                .autoFlush(properties.isAutoFlush())
                .flushIntervalMs(properties.getFlushIntervalMs())
                .build();
    }
}
