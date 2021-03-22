package org.fields.aiplatformmetadata.metadata.service.Impl;

import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.metadata.entity.Task;
import org.fields.aiplatformmetadata.metadata.entity.TaskInstance;
import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.fields.aiplatformmetadata.metadata.service.TaskService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@Component
public class InitlializeTaskPool implements InitializingBean {
    @Autowired
    ThreadPoolTaskScheduler threadPoolTaskScheduler;
    @Autowired
    Map<Long, ScheduledFuture<?>> taskPool;
    @Autowired
    TaskService taskService;
    @Autowired
    AutowireCapableBeanFactory autowireCapableBeanFactory;

    @Override
    public void afterPropertiesSet() throws Exception{
        List<Task> tasks = taskService.queryRunningTasks();
        for(Task task: tasks){
            String parameter = task.getParameter();
            Long id = task.getId();
            TaskInstance taskInstance = new TaskInstance(parameter);
            autowireCapableBeanFactory.autowireBean(taskInstance);
            ScheduledFuture<?> future = threadPoolTaskScheduler.schedule(taskInstance, new CronTrigger("0 0 1 * * ?"));//每天凌晨一点执行一次
            taskPool.put(id, future);
        }
        log.info("restore running task. taskPool size:{}", taskPool.size());
    }
}
