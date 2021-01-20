package org.fields.aiplatformmetadata.metadata.service;

import java.text.ParseException;
import java.util.*;

public interface TableService {
    Map<String, String> rootTables = new HashMap<String, String>(){{
        put("行情_A股", "wind_AShareEODPrices");
        put("行情期货", "wind_CCommidityFuturesEODPrices");
    }};
    boolean checkAndInitRootTables();
    boolean deleteRootTables();

    boolean createTableBase(String tableName, List<String> columns, List<String> columnTypes);
    boolean createTable(String oldTableName, String newTableName, String functionName, String updateTime, String updateUser, List<String> windColumns, List<String> dbColumns, List<String> userColumns, List<String> types) throws Exception;
    boolean deleteTable(String tableName);

    boolean addNewColumn(String tableName, String newWindColumn, String newDbColumn, String newUserColumn, String newColumnType) throws Exception;

    boolean synchronizeOneDayData(String oldTableName, String newTableName, String windColumn, Date date);
    boolean synchronizeRangeData(String oldTableName, String newTableName, String windColumn, String startStr, String endStr) throws ParseException;
}
