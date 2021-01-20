package org.fields.aiplatformmetadata.metadata.service.Impl;

import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.exception.ApiException;
import org.fields.aiplatformmetadata.metadata.Utils;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
@Service
public class TableServiceImpl implements TableService {
    @Autowired
    MetadataService metadataService;
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

    private List<Map<String, String>> list1 = new ArrayList<Map<String, String>>(){{
        add(new HashMap<String, String>(){{
            put("tableName", "wind_AShareEODPrices");
            put("windColumn", "windcode");
            put("dbColumn", "s_info_windcode");
            put("userColumn", "wind代码");
            put("type", "varchar(10)");
        }});
    }};
    private List<Map<String, String>> list2 = new ArrayList<Map<String, String>>(){{
        add(new HashMap<String, String>(){{
            put("tableName", "wind_CCommidityFuturesEODPrices");
            put("windColumn", "windcode");
            put("dbColumn", "s_info_windcode");
            put("userColumn", "wind代码");
            put("type", "varchar(10)");
        }});
    }};

    /**
     * 初始化十三个历史数据库表，在最开始调用一次即可
     * @return boolean
     */
    @Override
    public boolean checkAndInitRootTables() {
        boolean status = true;
        if(metadataService.isTableExisting(rootTables.get("行情_A股")) == false) {
            log.info("{} is not existing, create it.", "行情_A股");
            status = status && metadataService.insertTableMetadata(rootTables.get("行情_A股"), "wsd", "20190601", "zzt1");
            status = status && metadataService.insertTableMetadataDetail(list1);
            status = status && createTableBase(rootTables.get("行情_A股"),
                    new ArrayList<String>() {{
                        add(list1.get(0).get("dbColumn"));
                    }},
                    new ArrayList<String>() {{
                        add(list1.get(0).get("type"));
                    }});
        }

        if(metadataService.isTableExisting(rootTables.get("行情期货")) == false) {
            log.info("{} is not existing, create it", "行情期货");
            status = status && metadataService.insertTableMetadata(rootTables.get("行情期货"), "wsd", "20190601", "zzt2");
            status = status && metadataService.insertTableMetadataDetail(list2);
            status = status && createTableBase(rootTables.get("行情期货"),
                    new ArrayList<String>() {{
                        add(list2.get(0).get("dbColumn"));
                    }},
                    new ArrayList<String>() {{
                        add(list2.get(0).get("type"));
                    }});
        }
        return status;
    }

    /**
     * 删除所有数据库表格，用于单元测试，一般情况下不要用它
     * @return boolean
     */
    @Override
    public boolean deleteRootTables() {
        boolean status = metadataService.deleteTableMetadata(rootTables.get("行情_A股"));
        status = status && metadataService.deleteTableMetadataDetail(rootTables.get("行情_A股"));
        status = status && deleteTable(rootTables.get("行情_A股"));
        status = status && metadataService.deleteTableMetadata(rootTables.get("行情期货"));
        status = status && metadataService.deleteTableMetadataDetail(rootTables.get("行情期货"));
        status = status && deleteTable(rootTables.get("行情期货"));
        return status;
    }

    /**
     * @param tableName 数据库表名字
     * @param columns 数据库表每列的名字
     * @param columnTypes 数据库表每列的类型
     * @return boolean
     */
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
     * @param newTableName 这个是用户创建的表的名字，如果已经存在则上抛exception
     * @param functionName 这个表所使用的wind函数类型
     * @param updateTime 最后一次更新该表的时间
     * @param updateUser 最后一次更新该表的用户
     * @param windColumns wind里对应的field
     * @param dbColumns 在数据库中存储该列的列名,可以为null
     * @param userColumns 用户看到的该列的列名，默认情况下和dbColumns相同，可以自定义
     * @return boolean
     */
    @Override
    public boolean createTable(String oldTableName, String newTableName, String functionName, String updateTime, String updateUser, List<String> windColumns, List<String> dbColumns, List<String> userColumns, List<String> types) throws Exception{
        if(!metadataService.isTableExisting(oldTableName)){
            log.info("createTable error: old table {} is not existing", oldTableName);
            throw new ApiException("createTable error: old table is not existing");
        }
        if(metadataService.isTableExisting(newTableName)){
            log.info("createTable error: new table {} is already existing", newTableName);
            throw new ApiException("createaTable error: new Table is already existing");
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
                Assert.isTrue(metadataService.updateMetadata(oldTableName, updateTime, updateUser), "update success");
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

    /**
     * @param tableName 数据库表名
     * @return boolean
     */
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

    /**
     * @param tableName 数据库表名
     * @param newWindColumn 新增加的一列的wind field
     * @param newDbColumn 新增加的一列在数据库中的列名
     * @param newUserColumn 新增加的一列被用户看见的列名
     * @param newColumnType 新增加的一列的类型
     * @return boolean
     * @throws Exception mysql执行上抛的exception
     */
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

    @Override
    public boolean synchronizeOneDayData(String oldTableName, String newTableName, String windColumn, Date date) throws Exception{
        String oldDbColumn = metadataService.windColumn2DbColumn(oldTableName, windColumn);
        String newDbColumn = metadataService.windColumn2DbColumn(oldTableName, windColumn);
        String dateStr = simpleDateFormat.format(date);
        Set<String> existingWindCodes = Utils.getExistingWindCode(oldTableName, oldDbColumn, newDbColumn, dateStr);

        return false;
    }

    @Override
    public boolean synchronizeTimeRangeData(String oldTableName, String newTableName, String windColumn, String startStr, String endStr) throws Exception {
        Date startDate = simpleDateFormat.parse(startStr), endDate = simpleDateFormat.parse(endStr);
        Calendar c = Calendar.getInstance();
        c.setTime(endDate);
        c.add(Calendar.DATE, 1);
        endDate = c.getTime();
        c.clear();
        boolean status = true;
        for(Date cur = startDate; !cur.equals(endDate); cur = c.getTime()){
            log.info("synchronize date: {}", cur);
            status = status && synchronizeOneDayData(oldTableName, newTableName, windColumn, cur);
            c.setTime(cur);
            c.add(Calendar.DATE, 1);
        }
        return status;
    }

}
