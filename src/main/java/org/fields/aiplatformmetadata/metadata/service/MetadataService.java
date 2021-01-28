package org.fields.aiplatformmetadata.metadata.service;

import org.fields.aiplatformmetadata.metadata.entity.Metadata;
import org.fields.aiplatformmetadata.metadata.entity.MetadataDetail;

import java.util.List;
import java.util.Map;

public interface MetadataService {
    boolean isTableExisting(String tableName);

    String getWindCodeForDbColumn(String tableName);
    String getTradeDtForDbColumn(String tableName);
    String getType(String tableName, String windColumn);
    String getFunctionName(String tableName);

    Metadata queryMetadata(String tableName);
    List<MetadataDetail> queryMetadataDetails(String tableName);
    List<String> queryColumnsOfTable(String tableName);


    boolean isColumnExist(String tableName, String windColumn);

    MetadataDetail windColumn2MetadataDetail(String tableName, String windColumn);
    String windColumn2DbColumn(String tableName, String windColumn);

    boolean insertTableMetadata(String tableName, String func, String updateTime, String updateUser);
    boolean insertTableMetadataOneDetail(String tableName, String windColumn, String dbcolumn, String userColumn, String type);
    boolean insertTableMetadataDetail(List<Map<String, String>> list);
    boolean insertTask(String userName, String tableName, String description);

    boolean deleteTableMetadata(String tableName);
    boolean deleteTableMetadataDetail(String tableName);
    boolean deleteTask(String userName, String tableName);

    boolean updateMetadata(String tableName, String updateTime, String updateUser);
}
