package org.fields.aiplatformmetadata.metadata.service;

public interface DataService {
    // 行情表
    boolean isLineExisting(String tableName, String windCode, String dateStr);
    // dataset
    boolean isLineExisting(String tableName, String windCode);

    String getDataFromWind(String windCode, String dateStr, String windColumn);

    boolean updateData(String tableName, String windCode, String dateStr, String dbColumn, String value);
    boolean updateData(String tableName, String windCode, String dbColumn, String value);
    boolean insertData(String tableName, String windCode, String dateStr, String dbColumn, String value);
}
