package org.fields.aiplatformmetadata.metadata.service;

import java.util.Map;
import java.util.Set;

public interface MetadataService {
    boolean insertTableMetadata(String tableName, String func, String updateTime, String updateUser);
    boolean insertTableMetadataDetail(Set<Map<String, String>> set);
    boolean insertTask(String userName, String tableName, String description);

    boolean deleteTableMetadata(String tableName);
    boolean deleteTableMetadataDetail(String tableName);
    boolean deleteTask(String userName, String tableName);
}
