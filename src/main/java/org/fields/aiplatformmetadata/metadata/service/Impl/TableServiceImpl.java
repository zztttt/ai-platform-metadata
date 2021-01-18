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
        List<String> list3 = new ArrayList<String>(){{
            add(list1.get(0).get("dbColumn"));
        }};
        boolean status = true;
        status = status && metadataService.insertTableMetadata(rootTables.get("行情_A股"), "wsd", "20190601", "zzt1");
        status = status && metadataService.insertTableMetadataDetail(list1);
        status = status && createTableBase(rootTables.get("行情_A股"),
                new ArrayList<String>(){{ add(list1.get(0).get("dbColumn"));}},
                new ArrayList<String>(){{ add(list1.get(0).get("type"));}});

        status = status && metadataService.insertTableMetadata(rootTables.get("行情期货"), "wsd", "20190601", "zzt2");
        status = status && metadataService.insertTableMetadataDetail(list2);
        status = status && createTableBase(rootTables.get("行情期货"),
                new ArrayList<String>(){{ add(list2.get(0).get("dbColumn"));}},
                new ArrayList<String>(){{ add(list2.get(0).get("type"));}});
        return status;
    }

    @Override
    public boolean deleteRootTables() {
        boolean status = true;
        status = status && metadataService.deleteTableMetadata(rootTables.get("行情_A股"));
        status = status && metadataService.deleteTableMetadataDetail(rootTables.get("行情_A股"));
        status = status && deleteTable(rootTables.get("行情_A股"));
        status = status && metadataService.deleteTableMetadata(rootTables.get("行情期货"));
        status = status && metadataService.deleteTableMetadataDetail(rootTables.get("行情期货"));
        status = status && deleteTable(rootTables.get("行情期货"));
        return status;
    }

    @Override
    public boolean createTableBase(String tableName, List<String> columns, List<String> columnTypes) {
        log.info("createTableBase: {}, {} columns", tableName, columns.size());
        try{
            return Utils.createTable(tableName, columns, columnTypes);
        }catch (Exception e){
            log.info("createTableBase: {}, {} columns error", tableName, columns.size());
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
    public boolean createTable(String oldTableName, String newTableName, String functionName, String updateTime, String updateUser, List<String> windColumns, List<String> dbColumns, List<String> userColumns, List<String> types) throws Exception{
        if(!metadataService.isTableExisting(oldTableName)){
            log.info("createTable error: old table {} is not existing", oldTableName);
            throw new ApiException("createTable error: old table is not existing");
        }
        List<Map<String, String>> metadataDetails = new ArrayList<>();
        int len = userColumns.size();
        for(int i = 0; i < len; ++i){
            String userColumn = userColumns.get(i), windColumn = windColumns.get(i), dbColumn = dbColumns.get(i), type = types.get(i);
            // trigger addNewColumn
            if(metadataService.isColumnExist(oldTableName, windColumn) == false){
                log.info("table {} windColumn {} is not existing, trigger addNewColumn", oldTableName, windColumn);
                if(dbColumn == null){
                    dbColumn = userColumn;
                    dbColumns.set(i, userColumn);
                }
                addNewColumn(oldTableName, windColumn, dbColumn, userColumn, type);
                // TODO metadataService.updateMetadata
            }
            // add the metadataDetail
            String finalDbColumn = dbColumn;
            metadataDetails.add(new HashMap<String, String>(){{
                put("tableName", newTableName);
                put("windColumn", windColumn);
                put("dbColumn", finalDbColumn);
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
        log.info("deleteTable: {}", tableName);
        try {
            Utils.deleteTable(tableName);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addNewColumn(String tableName, String newWindColumn, String newDbColumn, String newUserColumn, String newColumnType) throws Exception{
        log.info("addNewColumn: {} in table {}", newWindColumn, tableName);
        if(metadataService.isTableExisting(tableName) == false){{
            log.info("addNewColumn error. table {} is not existing", tableName);
            throw new ApiException("addNewColumn error");
        }}
        boolean status = metadataService.insertTableMetadataOneDetail(tableName, newWindColumn, newDbColumn, newUserColumn, newColumnType);
        Utils.addNewColumn(tableName, newDbColumn, newColumnType);
        return status;
    }

}
