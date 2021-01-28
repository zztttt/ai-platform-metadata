package org.fields.aiplatformmetadata.metadata.service;

import java.util.List;
import java.util.Map;

public interface DataService {
    // 行情表
    boolean isLineExisting(String tableName, String windCode, String dateStr);

    List<Map<String, Object>> queryOneLineFromCache(String oldTableName, String windCode, String dateStr);
}
