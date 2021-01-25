package org.fields.aiplatformmetadata.metadata.service;

import org.fields.aiplatformmetadata.metadata.entity.Task;

public interface TaskService {
    boolean insertNewTask(String userName, String tableName, String description);
    Task queryTask(String userName, String tableName);
    boolean deleteTask(String userName, String tableName);
}
