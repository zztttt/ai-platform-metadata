package org.fields.aiplatformmetadata.metadata.service;

import java.util.Set;

public interface TableService {
    boolean createTable(String oldTableName, String newTableName, String functionName, String updateTime, String updateUser, Set<String> userColumns) throws Exception;
    boolean deleteTable(String tableName);
}
