package org.fields.aiplatformmetadata.metadata.service;

import java.util.List;
import java.util.Map;

public interface DataService {
    boolean isLineExisting(String tableName, String windCode, String dateStr);

    List<Map<String, Object>> queryOneLineFromCache(String oldTableName, String windCode, String dateStr);

    String getDataFromWind(String windCode, String dateStr, String windColumn);

    boolean updateData(String tableName, String windCode, String dateStr, String windColumn, String value);
    boolean insertData(String tableName, String windCode, String dateStr, String windColumn, String value);
}
