package org.fields.aiplatformmetadata.metadata.service.Impl;

import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.exception.ApiException;
import org.fields.aiplatformmetadata.metadata.FailUtils;
import org.fields.aiplatformmetadata.metadata.Utils;
import org.fields.aiplatformmetadata.metadata.entity.MetadataDetail;
import org.fields.aiplatformmetadata.metadata.service.DataService;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
@Service
public class TableServiceImpl implements TableService {
    @Autowired
    MetadataService metadataService;
    @Autowired
    DataService dataService;
    @Autowired
    Utils utils;

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
     * @param tableName 数据库表名字
     * @param columns 数据库表每列的名字
     * @param columnTypes 数据库表每列的类型
     * @return boolean
     */
    @Override
    public boolean createTableBase(String tableName, List<String> columns, List<String> columnTypes) {
        log.info("createTableBase: {}, {} columns", tableName, columns.size());
        try{
            return FailUtils.createTable(tableName, columns, columnTypes);
        }catch (Exception e){
            log.info("createTableBase: {}, {} columns error", tableName, columns.size());
            e.printStackTrace();
            return false;
        }
    }
    @Override
    public Boolean createTable(String oldTableName, String newTableName, String updateTime, String updateUser, String startStr, String endStr, List<String> windCodes, List<String> windColumns, List<String> dbColumns, List<String> userColumns) {
        if(!metadataService.isTableExisting(oldTableName)){
            log.info("createTable error: old table {} is not existing", oldTableName);
            throw new ApiException("createTable error: old table is not existing");
        }
        if(metadataService.isTableExisting(newTableName)){
            log.info("createTable error: new table {} is already existing", newTableName);
            throw new ApiException("createTable error: new Table is already existing");
        }
        String functionName = metadataService.getFunctionName(oldTableName);
        List<String> types = new ArrayList<>();
        for(String windColumn: windColumns){
            String type = metadataService.getType(oldTableName, windColumn);
            types.add(type);
        }
        List<Map<String, String>> metadataDetails = new ArrayList<>();
        int len = windColumns.size();
        for(int i = 0; i < len; ++i){
            String userColumn = userColumns.get(i), windColumn = windColumns.get(i), dbColumn = dbColumns.get(i), type = types.get(i);
            // add the metadataDetail
            metadataDetails.add(new HashMap<String, String>(){{
                put("tableName", newTableName);
                put("windColumn", windColumn);
                put("dbColumn", dbColumn);
                put("userColumn", userColumn);
                put("type", type);
            }});
        }
        boolean ret = utils.createTable(newTableName, dbColumns, types);
        ret = ret && metadataService.insertTableMetadata(newTableName, functionName, updateTime, updateUser);
        ret = ret && metadataService.insertTableMetadataDetail(metadataDetails);
        ret = ret && synchronizeCodes(oldTableName, newTableName, windCodes, startStr, endStr);

        return ret;
    }

    @Override
    public Boolean synchronizeCodes(String oldTableName, String newTableName, List<String> windCodes, String startStr, String endStr) {
        Boolean status = true;
        for(String windCode: windCodes){
            status = status && synchronizeCode(oldTableName, newTableName, windCode, startStr, endStr);
        }
        return status;
    }

    @Override
    public Boolean synchronizeCode(String oldTableName, String newTableName, String windCode, String startStr, String endStr) {
        try{
            Date startDate = simpleDateFormat.parse(startStr), endDate = simpleDateFormat.parse(endStr);
            Calendar c = Calendar.getInstance();
            c.setTime(endDate);
            c.add(Calendar.DATE, 1);
            endDate = c.getTime();
            c.clear();
            Boolean status = true;

            // 有该天的数据，但不全，需要从wind拿
            // <date, [columns] > >
            Map<String, List<String>> emptyCell = new HashMap<>();
            for(Date cur = startDate; !cur.equals(endDate); cur = c.getTime()){
                log.info("synchronize windCode: {}", windCode);
                String dateStr = simpleDateFormat.format(cur);

                // cache里有数据，优先从cache中拿
                if(dataService.isLineExisting(oldTableName, windCode, dateStr)){
                    log.info("cache hit");
                    List<Map<String, Object>> result = dataService.queryOneLineFromCache(oldTableName, windCode, dateStr);
                    if(result.size() > 1){
                        log.info("more than one line data");
                        throw new ApiException("more than one line data");
                    }
                    Map<String, Object> map = result.get(0);
                    List<String> emptyColumn = new ArrayList<>();
                    // windColumn - value
                    for(Map.Entry<String, Object> entry: map.entrySet()){
                        log.info("pair: {}", entry);
                        String windColumn = entry.getKey();
                        if(entry.getValue() == null && metadataService.isColumnExist(newTableName, windColumn)){
                            emptyColumn.add(entry.getKey());
                        }
                    }
                    emptyCell.put(dateStr, emptyColumn);
                }else{
                    // cache 里没数据，整天都要从wind拿
                    log.info("cache miss");
                    // 先查wind 如果有这么一天再进行更新

                    List<String> emptyColumn = metadataService.queryColumnsOfTable(newTableName);
                    emptyCell.put(dateStr, emptyColumn);
                }

                // 进行wind数据更新

                c.setTime(cur);
                c.add(Calendar.DATE, 1);
            }
            return status;
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean FailCreateTable(String oldTableName, String newTableName, String functionName, String updateTime, String updateUser, String startStr, String endStr, List<String> windColumns, List<String> dbColumns, List<String> userColumns, List<String> types) throws Exception{
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

        boolean ret = FailUtils.createTable(newTableName, dbColumns, types);
        ret = ret && metadataService.insertTableMetadata(newTableName, functionName, updateTime, updateUser);
        ret = ret && metadataService.insertTableMetadataDetail(metadataDetails);
        ret = ret && synchronizeAllData(oldTableName, newTableName, windColumns, startStr, endStr);

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
            FailUtils.deleteTable(tableName);
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
        FailUtils.addNewColumn(tableName, newDbColumn, newColumnType);
        return status;
    }

    @Override
    public boolean synchronizeOneDayData(String oldTableName, String newTableName, String windColumn, Date date) throws Exception{
        String oldDbColumn = metadataService.windColumn2DbColumn(oldTableName, windColumn);
        String newDbColumn = metadataService.windColumn2DbColumn(oldTableName, windColumn);
        String dateStr = simpleDateFormat.format(date);
        List<MetadataDetail> metadataDetails = metadataService.queryMetadataDetails(oldTableName);
        String tradeDt = null;
        for(MetadataDetail metadataDetail: metadataDetails){
            //日行情表
            if(metadataDetail.getDbColumn().equals("trade_dt")){
                tradeDt = metadataDetail.getDbColumn();
            }
        }
        if(tradeDt != null){
            boolean status = true;
            Set<String> existingWindCodes = FailUtils.getExistingWindCode(oldTableName, tradeDt, dateStr);
            for(String windCode: existingWindCodes){
                String value = FailUtils.getData(oldTableName, windCode, dateStr, oldDbColumn);
                // cache 中没有数据，从wind拿
                if(value == null){
                    value = dataService.getDataFromWind(windCode, dateStr, windColumn);
                    // 然后将该数据先写入oldTable，再写入newTable
                    status = status && dataService.updateData(oldTableName, windCode, dateStr, oldDbColumn, value);
                }
                status = status && dataService.updateData(newTableName, windCode, dateStr, newDbColumn, value);
            }
            return status;
        }else{
            // TODO
            return false;
        }
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

    @Override
    public boolean synchronizeAllData(String oldTableName, String newTableName, List<String> windColumns, String startStr, String endStr) throws Exception{
        boolean status = true;
        for(String windColumn: windColumns){
            if(!windColumn.equals("windcode"))
                status = status && synchronizeTimeRangeData(oldTableName, newTableName, windColumn, startStr, endStr);
        }
        return status;
    }

}
