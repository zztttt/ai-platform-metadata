package org.fields.aiplatformmetadata.metadata.service;

public interface DataService {
    boolean isLineExisting(String tableName, String windCode, String dateStr);

    String getDataFromWind(String windCode, String dateStr, String windColumn);

    boolean updateData(String tableName, String windCode, String dateStr, String windColumn, String value);
    boolean insertData(String tableName, String windCode, String dateStr, String windColumn, String value);
}
