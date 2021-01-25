package org.fields.aiplatformmetadata.metadata.service.Impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.exception.ApiException;
import org.fields.aiplatformmetadata.metadata.entity.Task;
import org.fields.aiplatformmetadata.metadata.mapper.TaskMapper;
import org.fields.aiplatformmetadata.metadata.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.xml.ws.ServiceMode;

@Slf4j
@Service
public class TaskServiceImpl implements TaskService {
    @Autowired
    TaskMapper taskMapper;

    @Override
    public boolean insertNewTask(String userName, String tableName, String description) {
        Task task = new Task();
        task.setUsername(userName);
        task.setTableName(tableName);
        task.setDescription(description);
        int ret = taskMapper.insert(task);
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
}
