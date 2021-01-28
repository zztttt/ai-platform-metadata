package org.fields.aiplatformmetadata.metadata.service;

import java.text.ParseException;
import java.util.*;

public interface TableService {
    Map<String, String> rootTables = new HashMap<String, String>(){{
        put("行情_A股", "wind_AShareEODPrices_test");
        put("行情期货", "wind_CCommidityFuturesEODPrices_test");
        put("A股基本资料", "wind_AShareDescription_test");
    }};

    Boolean createTable(String oldTableName, String newTableName, String updateTime, String updateUser, String startStr, String endStr, List<String> windCodes, List<String> windColumns, List<String> dbColumns, List<String> userColumns);
    Boolean synchronizeCodes(String oldTableName, String newTableName, List<String> windCodes, String startStr, String endStr);
    Boolean synchronizeCode(String oldTableName, String newTableName, String windCode, String startStr, String endStr);
}
