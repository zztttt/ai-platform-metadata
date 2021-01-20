package org.fields.aiplatformmetadata.metadata.service.Impl;

import org.fields.aiplatformmetadata.metadata.service.DataService;

public class DataServiceImpl implements DataService {

    @Override
    public boolean isLineExisting(String tableName, String windCode, String dateStr) {
        return false;
    }

    @Override
    public boolean getDataFromCache(String tableName, String windCode, String dbColumn, String dateStr) {
        return false;
    }

    @Override
    public boolean getDataFromWind(String tableName, String dbColumn) {
        return false;
    }
}
