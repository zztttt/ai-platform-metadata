package org.fields.aiplatformmetadata.metadata.service;

import java.util.List;
import java.util.Map;

public interface DataService {
    // 带时间序列
    boolean isLineExisting(String tableName, String windCode, String dateStr) throws Exception;
    // 不带时间序列
    Boolean isLineExisting(String tableName, String windCode) throws Exception;

    // 带时间序列
    List<Map<String, Object>> queryOneLineFromCache(String oldTableName, String windCode, String dateStr) throws Exception;
    // 不带时间序列
    List<Map<String, Object>> queryOneLineFromCache(String oldTableName, String windCode) throws Exception;
}
