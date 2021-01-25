package org.fields.aiplatformmetadata.metadata.service;

import java.text.ParseException;
import java.util.*;

public interface TableService {
    Map<String, String> rootTables = new HashMap<String, String>(){{
        put("行情_A股", "wind_AShareEODPrices_test");
        put("行情期货", "wind_CCommidityFuturesEODPrices_test");
        put("A股基本资料", "wind_AShareDescription_test");
    }};
    boolean checkAndInitRootTables();
    boolean deleteRootTables();

    boolean createTableBase(String tableName, List<String> columns, List<String> columnTypes);
    boolean createTable(String oldTableName, String newTableName, String functionName, String updateTime, String updateUser, String startStr, String endStr, List<String> windColumns, List<String> dbColumns, List<String> userColumns, List<String> types) throws Exception;
    boolean deleteTable(String tableName);

    boolean addNewColumn(String tableName, String newWindColumn, String newDbColumn, String newUserColumn, String newColumnType) throws Exception;

    // 行情表
    boolean synchronizeOneDayData(String oldTableName, String newTableName, String windColumn, Date date, List<String> existingWindCodes) throws Exception;
    boolean synchronizeTimeRangeData(String oldTableName, String newTableName, String windColumn, String startStr, String endStr) throws Exception;
    boolean synchronizeAllData(String oldTableName, String newTableName, List<String> windColumns, String startStr, String endStr) throws Exception;

    // 数据集
    boolean synchronizeDataset(String oldTableName, String newTableName, List<String> windCodes, List<String> windColumns, String startStr, String endStr) throws Exception;
}
