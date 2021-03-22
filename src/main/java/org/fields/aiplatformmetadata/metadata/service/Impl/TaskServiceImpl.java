package org.fields.aiplatformmetadata.metadata.service.Impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import io.swagger.models.auth.In;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.exception.ApiException;
import org.fields.aiplatformmetadata.metadata.entity.Task;
import org.fields.aiplatformmetadata.metadata.mapper.TaskMapper;
import org.fields.aiplatformmetadata.metadata.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.xml.ws.ServiceMode;
import java.util.List;

@Slf4j
@Service
public class TaskServiceImpl implements TaskService {
    @Autowired
    TaskMapper taskMapper;

    @Override
    public boolean insertNewTask(String userName, String tableName, String description, String parameter, String status) throws Exception{
        Task task = new Task();
        task.setUsername(userName);
        task.setTableName(tableName);
        task.setDescription(description);
        task.setParameter(parameter);
        task.setStatus(status);
        int ret = taskMapper.insert(task);
        if(ret == 0){
            log.error("insert new task error.");
            return false;
        }
        return ret == 1;
    }

    @Override
    public Task queryTask(String userName, String tableName) {
        QueryWrapper<Task> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("username", userName).eq("tableName", tableName);
        Task task = taskMapper.selectOne(queryWrapper);
        if(task == null){
            log.info("query task null");
            throw new ApiException("query task null");
        }
        return task;
    }

    @Override
    public boolean deleteTask(String userName, String tableName) {
        UpdateWrapper<Task> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("username", userName).eq("tableName", tableName);
        int ret = taskMapper.delete(updateWrapper);
        return ret == 1;
    }

    @Override
    public Long queryTaskId(String tableName) {
        QueryWrapper<Task> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("tableName", tableName);
        Task task = taskMapper.selectOne(queryWrapper);
        if(task == null){
            log.info("query task id is  null");
            throw new ApiException("query task id is null");
        }
        return task.getId();
    }

    @Override
    public List<Task> queryRunningTasks() {
        QueryWrapper<Task> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("status", "running");
        List<Task> tasks = taskMapper.selectList(queryWrapper);
        if(tasks == null){
            log.info("query running task is  null");
            throw new ApiException("query running task is null");
        }
        return tasks;
    }
}
