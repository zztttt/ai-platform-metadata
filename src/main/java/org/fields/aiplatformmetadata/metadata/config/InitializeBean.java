package org.fields.aiplatformmetadata.metadata.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.*;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@Configuration
public class InitializeBean {
    @Bean
    public ThreadPoolTaskScheduler threadPoolTaskScheduler(){
        log.info("create threadPoolTaskScheduler");
        return new ThreadPoolTaskScheduler();
    }
    @Bean
    public Map<Long, ScheduledFuture<?>> tasks(){
        log.info("create task pool");
        return new Hashtable<>();
    }
}
