package org.fields.aiplatformmetadata.metadata.service.Impl;

import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.exception.ApiException;
import org.fields.aiplatformmetadata.metadata.Utils;
import org.fields.aiplatformmetadata.metadata.entity.Metadata;
import org.fields.aiplatformmetadata.metadata.mapper.MetadataMapper;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class TableServiceImpl implements TableService {
    @Autowired
    MetadataService metadataService;

    @Override
    public boolean initRootTables() {
        List<Map<String, String>> list1 = new ArrayList<Map<String, String>>(){{
            add(new HashMap<String, String>(){{
                put("tableName", "wind_AShareEODPrices");
                put("windColumn", "windcode");
                put("dbColumn", "s_info_windcode");
                put("userColumn", "wind代码");
                put("type", "varchar(10)");
            }});
        }};
        List<Map<String, String>> list2 = new ArrayList<Map<String, String>>(){{
           add(new HashMap<String, String>(){{
               put("tableName", "wind_CCommidityFuturesEODPrices");
               put("windColumn", "windcode");
               put("dbColumn", "s_info_windcode");
               put("userColumn", "wind代码");
               put("type", "varchar(10)");
            }});
        }};
        boolean status = true;
        status = status && metadataService.insertTableMetadata(rootTables.get("行情_A股"), "wsd", "20190601", "zzt1");
        status = status && metadataService.insertTableMetadataDetail(list1);
        status = status && metadataService.insertTableMetadata(rootTables.get("行情期货"), "wsd", "20190601", "zzt2");
        status = status && metadataService.insertTableMetadataDetail(list2);
        return status;
    }

    @Override
    public boolean deleteRootTables() {
        boolean status = true;
        status = status && metadataService.deleteTableMetadata(rootTables.get("行情_A股"));
        status = status && metadataService.deleteTableMetadataDetail(rootTables.get("行情_A股"));
        status = status && metadataService.deleteTableMetadata(rootTables.get("行情期货"));
        status = status && metadataService.deleteTableMetadataDetail(rootTables.get("行情期货"));
        return status;
    }

    @Override
    public boolean createTableBase(String tableName, List<String> columns, List<String> columnTypes) {
        try{
            return Utils.createTable(tableName, columns, columnTypes);
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * @param oldTableName 这个必须是已经存在的表，用户只允许从最大的表选择指定的列来创建新表
     * @param functionName
     * @param updateTime
     * @param updateUser
     * @param userColumns
     * @return
     */
    @Override
    public boolean createTable(String oldTableName, String newTableName, String functionName, String updateTime, String updateUser, Set<String> userColumns) throws Exception{
        if(!metadataService.isTableExisting(oldTableName)){
            log.info("createTable error: old table {} is not existing", oldTableName);
            throw new ApiException("createTable error: old table is not existing");
        }
        ArrayList<String> dbColumns = new ArrayList<>();
        ArrayList<String> types = new ArrayList<>();
        List<Map<String, String>> metadataDetails = new ArrayList<>();
        for(String userColumn: userColumns){
            String windColumn = metadataService.queryWindColumnFromUserColumn(oldTableName, userColumn);
            String dbColumn = metadataService.queryDbColumnFromUserColumn(oldTableName, userColumn);
            String type = metadataService.queryTypeFromUserColumn(oldTableName, userColumn);

            dbColumns.add(dbColumn);
            types.add(type);
            metadataDetails.add(new HashMap<String, String>(){{
                put("tableName", newTableName);
                put("windColumn", windColumn);
                put("dbColumn", dbColumn);
                put("userColumn", userColumn);
                put("type", type);
            }});
        }

        boolean ret = Utils.createTable(newTableName, dbColumns, types);
        ret = ret && metadataService.insertTableMetadata(newTableName, functionName, updateTime, updateUser);
        ret = ret && metadataService.insertTableMetadataDetail(metadataDetails);

        return ret;
    }

    @Override
    public boolean deleteTable(String tableName){
        try {
            Utils.deleteTable(tableName);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }
}
