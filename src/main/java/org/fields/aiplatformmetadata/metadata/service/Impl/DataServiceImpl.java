package org.fields.aiplatformmetadata.metadata.service.Impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.metadata.SqlUtils;
import org.fields.aiplatformmetadata.metadata.Utils;
import org.fields.aiplatformmetadata.metadata.entity.MetadataDetail;
import org.fields.aiplatformmetadata.metadata.service.DataService;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class DataServiceImpl implements DataService {
    @Autowired
    MetadataService metadataService;

    @Override
    public boolean isLineExisting(String tableName, String windCode, String dateStr) {
        String windDbColumn = metadataService.getWindCodeForDbColumn(tableName);
        String dateDbColumn = metadataService.getTradeDtForDbColumn(tableName);
        return Utils.isLineExisting(tableName, windDbColumn, dateDbColumn, windCode, dateStr);
    }

    @Override
    public String getDataFromWind(String windCode, String dateStr, String windColumn) {
        String[] args = new String[3];
        args[0] = windCode;
        args[1] = dateStr;
        args[2] = windColumn;
        return Utils.callScript(args);
    }

    @Override
    public boolean updateData(String tableName, String windCode, String dateStr, String windColumn, String value) {
        if(!isLineExisting(tableName, windCode, dateStr)){
            return Utils.insertData(tableName, windCode, dateStr, windColumn, value);
        }
        return Utils.updateData(tableName, windCode, dateStr, windColumn, value);
    }

    @Override
    public boolean insertData(String tableName, String windCode, String dateStr, String windColumn, String value) {
        return Utils.insertData(tableName, windCode, dateStr, windColumn, value);
    }
}
