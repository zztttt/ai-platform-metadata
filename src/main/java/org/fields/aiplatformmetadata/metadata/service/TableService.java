package org.fields.aiplatformmetadata.metadata.service;

import java.text.ParseException;
import java.util.*;

public interface TableService {
    Map<String, String> rootTables = new HashMap<String, String>(){{
        put("行情_A股", "wind_AShareEODPrices");
        put("行情期货", "wind_CCommidityFuturesEODPrices");
    }};

    Boolean createTable(String oldTableName, String newTableName, String updateTime, String updateUser, String startStr, String endStr, List<String> windCodes, List<String> windColumns, List<String> dbColumns, List<String> userColumns);
    Boolean synchronizeCodes(String oldTableName, String newTableName, List<String> windCodes, String startStr, String endStr);
    Boolean synchronizeCode(String oldTableName, String newTableName, String windCode, String startStr, String endStr);

    boolean createTableBase(String tableName, List<String> columns, List<String> columnTypes);
    boolean FailCreateTable(String oldTableName, String newTableName, String functionName, String updateTime, String updateUser, String startStr, String endStr, List<String> windColumns, List<String> dbColumns, List<String> userColumns, List<String> types) throws Exception;
    boolean deleteTable(String tableName);

    boolean addNewColumn(String tableName, String newWindColumn, String newDbColumn, String newUserColumn, String newColumnType) throws Exception;

    boolean synchronizeOneDayData(String oldTableName, String newTableName, String windColumn, Date date) throws Exception;
    boolean synchronizeTimeRangeData(String oldTableName, String newTableName, String windColumn, String startStr, String endStr) throws Exception;
    boolean synchronizeAllData(String oldTableName, String newTableName, List<String> windColumns, String startStr, String endStr) throws Exception;
}
