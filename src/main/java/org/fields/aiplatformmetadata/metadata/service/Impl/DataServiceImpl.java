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
    public boolean isLineExisting(String tableName, String windCode) {
        String windDbColumn = metadataService.getWindCodeForDbColumn(tableName);
        return Utils.isLineExisting(tableName, windDbColumn, windCode);
    }

    /**
     * 行情表wsd
     * @param windCode
     * @param dateStr
     * @param windColumn
     * @return
     */
    @Override
    public String getDataFromWind(String windCode, String dateStr, String windColumn) {
        String[] args = new String[3];
        args[0] = windCode;
        args[1] = dateStr;
        args[2] = windColumn;
        return Utils.callScript(args);
    }

    /**
     * 行情表
     * @param tableName
     * @param windCode
     * @param dateStr
     * @param dbColumn
     * @param value
     * @return
     */
    @Override
    public boolean updateData(String tableName, String windCode, String dateStr, String dbColumn, String value) {
        String windCodeColumn = metadataService.getWindCodeForDbColumn(tableName);
        String dateStrColumn = metadataService.getTradeDtForDbColumn(tableName);
        String dateWindColumn = metadataService.dbColumn2WindColumn(tableName, dbColumn);
        if(!isLineExisting(tableName, windCode, dateStr)){
            //log.info("=============================insert new line. dateStrColumn: {}", dateStrColumn);
            return Utils.insertNewLine(tableName, windCodeColumn, dateStrColumn, windCode, dateStr);
        }
        return Utils.updateData(tableName, windCodeColumn, dateStrColumn, windCode, dateStr, dbColumn, value);
    }

    /**
     * dataset
     * @param tableName
     * @param windCode
     * @param dbColumn
     * @param value
     * @return
     */
    @Override
    public boolean updateData(String tableName, String windCode, String dbColumn, String value) {
        String windCodeColumn = metadataService.getWindCodeForDbColumn(tableName);
        if(!isLineExisting(tableName, windCode)){
            return Utils.insertNewLine(tableName, windCodeColumn, windCode);
        }
        return Utils.updateData(tableName, windCodeColumn, windCode, dbColumn, value);
    }


    @Override
    public boolean insertData(String tableName, String windCode, String dateStr, String dbColumn, String value) {
        return Utils.insertData(tableName, windCode, dateStr, dbColumn, value);
    }
}
