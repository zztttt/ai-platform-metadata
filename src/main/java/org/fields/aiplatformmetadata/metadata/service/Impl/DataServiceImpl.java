package org.fields.aiplatformmetadata.metadata.service.Impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.metadata.SqlUtils;
import org.fields.aiplatformmetadata.metadata.Utils;
import org.fields.aiplatformmetadata.metadata.entity.MetadataDetail;
import org.fields.aiplatformmetadata.metadata.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class DataServiceImpl implements DataService {

    @Override
    public boolean isLineExisting(String tableName, String windCode, String dateStr) {
        return Utils.isLineExisting(tableName, windCode, dateStr);
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
