package org.fields.aiplatformmetadata.metadata.service.Impl;

import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.metadata.FailUtils;
import org.fields.aiplatformmetadata.metadata.Utils;
import org.fields.aiplatformmetadata.metadata.service.DataService;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class DataServiceImpl implements DataService {
    @Autowired
    MetadataService metadataService;
    @Autowired
    Utils utils;

    @Override
    public boolean isLineExisting(String tableName, String windCode, String dateStr) {
        String windCodeColumn = metadataService.getWindCodeForDbColumn(tableName);
        String dateDbColumn = metadataService.getTradeDtForDbColumn(tableName);
        return utils.isLineExisting(tableName, windCodeColumn, dateDbColumn, windCode, dateStr);
    }

    @Override
    public List<Map<String, Object>> queryOneLineFromCache(String oldTableName, String windCode, String dateStr) {
        String windCodeDbColumn = metadataService.getWindCodeForDbColumn(oldTableName);
        String dateDbColumn = metadataService.getTradeDtForDbColumn(oldTableName);
        return utils.queryOneLine(oldTableName, windCodeDbColumn, dateDbColumn, windCode, dateStr);
    }

    @Override
    public String getDataFromWind(String windCode, String dateStr, String windColumn) {
        String[] args = new String[3];
        args[0] = windCode;
        args[1] = dateStr;
        args[2] = windColumn;
        return FailUtils.callScript(args);
    }

    @Override
    public boolean updateData(String tableName, String windCode, String dateStr, String windColumn, String value) {
        if(!isLineExisting(tableName, windCode, dateStr)){
            return FailUtils.insertData(tableName, windCode, dateStr, windColumn, value);
        }
        return FailUtils.updateData(tableName, windCode, dateStr, windColumn, value);
    }

    @Override
    public boolean insertData(String tableName, String windCode, String dateStr, String windColumn, String value) {
        return FailUtils.insertData(tableName, windCode, dateStr, windColumn, value);
    }
}
