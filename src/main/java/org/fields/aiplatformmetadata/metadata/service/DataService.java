package org.fields.aiplatformmetadata.metadata.service;

public interface DataService {
    boolean isLineExisting(String tableName, String windCode, String dateStr);

    String getDataFromWind(String windCode, String dateStr, String windColumn);

    boolean updateData(String tableName, String windCode, String dateStr, String dbColumn, String value);
    boolean updateNewLine(String tableName, String windCodeColumn, String dateStrColumn, String windCode, String dateStr);
    boolean insertData(String tableName, String windCode, String dateStr, String dbColumn, String value);
}
