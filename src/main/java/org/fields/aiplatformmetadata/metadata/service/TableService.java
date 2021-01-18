package org.fields.aiplatformmetadata.metadata.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}
