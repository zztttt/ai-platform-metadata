package org.fields.aiplatformmetadata.metadata.service;

import org.fields.aiplatformmetadata.metadata.entity.Metadata;
import org.fields.aiplatformmetadata.metadata.entity.MetadataDetail;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MetadataService {
    boolean isTableExisting(String tableName);
    boolean isTaskExisting(String tableName);

    Metadata queryMetadata(String tableName);
    String queryDbColumnFromUserColumn(String tableName, String userColumn);
    String queryWindColumnFromUserColumn(String tableName, String userColumn);
    String queryTypeFromUserColumn(String tableName, String userColumn);

    boolean insertTableMetadata(String tableName, String func, String updateTime, String updateUser);
    boolean insertTableMetadataDetail(List<Map<String, String>> list);
    boolean insertTask(String userName, String tableName, String description);

    boolean deleteTableMetadata(String tableName);
    boolean deleteTableMetadataDetail(String tableName);
    boolean deleteTask(String userName, String tableName);
}
