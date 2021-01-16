package org.fields.aiplatformmetadata.metadata.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface TableService {
    Map<String, String> rootTables = new HashMap<String, String>(){{
        put("行情_A股", "AShareEODPrices");
        put("行情期货", "CCommodityFuturesEODPrices");
    }};
    boolean initRootTables();
    boolean createTableBase(String tableName, List<String> columns, List<String> columnTypes);
    boolean createTable(String oldTableName, String newTableName, String functionName, String updateTime, String updateUser, Set<String> userColumns) throws Exception;
    boolean deleteTable(String tableName);
}
