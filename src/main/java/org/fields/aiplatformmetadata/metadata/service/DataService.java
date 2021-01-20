package org.fields.aiplatformmetadata.metadata.service;

public interface DataService {
    boolean isLineExisting(String tableName, String windCode, String dateStr);
    boolean getDataFromCache(String tableName, String windCode, String dbColumn, String dateStr);
    boolean getDataFromWind(String tableName, String dbColumn);
}
