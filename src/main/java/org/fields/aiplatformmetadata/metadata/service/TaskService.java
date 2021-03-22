package org.fields.aiplatformmetadata.metadata.service;

import org.fields.aiplatformmetadata.metadata.entity.Task;

import java.util.List;

public interface TaskService {
    boolean insertNewTask(String userName, String tableName, String description, String parameter, String status) throws Exception;
    Task queryTask(String userName, String tableName);
    boolean deleteTask(String userName, String tableName);
    Long queryTaskId(String tableName);
    List<Task> queryRunningTasks();
}
