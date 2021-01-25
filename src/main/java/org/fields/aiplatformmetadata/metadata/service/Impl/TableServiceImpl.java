package org.fields.aiplatformmetadata.metadata.service.Impl;

import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.exception.ApiException;
import org.fields.aiplatformmetadata.metadata.Utils;
import org.fields.aiplatformmetadata.metadata.entity.MetadataDetail;
import org.fields.aiplatformmetadata.metadata.service.DataService;
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
    @Autowired
    DataService dataService;

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

    private List<Map<String, String>> list1 = new ArrayList<Map<String, String>>(){{
        add(new HashMap<String, String>(){{
            put("tableName", "wind_AShareEODPrices_test");
            put("windColumn", "windcode");
            put("dbColumn", "s_info_windcode");
            put("userColumn", "wind代码");
            put("type", "varchar(40)");
        }});
    }};
    private List<Map<String, String>> list2 = new ArrayList<Map<String, String>>(){{
        add(new HashMap<String, String>(){{
            put("tableName", "wind_CCommidityFuturesEODPrices_test");
            put("windColumn", "windcode");
            put("dbColumn", "s_info_windcode");
            put("userColumn", "wind代码");
            put("type", "varchar(40)");
        }});
    }};
    private List<Map<String, String>> list3 = new ArrayList<Map<String, String>>(){{
        add(new HashMap<String, String>(){{
            put("tableName", "wind_AShareDescription_test");
            put("windColumn", "windcode");
            put("dbColumn", "s_info_windcode");
            put("userColumn", "wind代码");
            put("type", "varchar(40)");
        }});
    }};

    private List<String> windCodes = new ArrayList<String>(){{
        add("000001.SZ");
        add("000002.SZ");
        add("000004.SZ");
        add("000005.SZ");
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
            status = status && createTableBase(
                    rootTables.get("行情期货"),
                    new ArrayList<String>() {{
                        add(list2.get(0).get("dbColumn"));
                    }},
                    new ArrayList<String>() {{
                        add(list2.get(0).get("type"));
                    }});
        }
        if(metadataService.isTableExisting(rootTables.get("A股基本资料")) == false) {
            log.info("{} is not existing, create it", "A股基本资料");
            status = status && metadataService.insertTableMetadata(rootTables.get("A股基本资料"), "wsd", "20190601", "zzt3");
            status = status && metadataService.insertTableMetadataDetail(list3);
            status = status && createTableBase(rootTables.get("A股基本资料"),
                    new ArrayList<String>() {{
                        add(list3.get(0).get("dbColumn"));
                    }},
                    new ArrayList<String>() {{
                        add(list3.get(0).get("type"));
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
    public boolean createTable(String oldTableName, String newTableName, String functionName, String updateTime, String updateUser, String startStr, String endStr, List<String> windColumns, List<String> dbColumns, List<String> userColumns, List<String> types) throws Exception{
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
        if(oldTableName.equals("wind_AShareEODPrices_test") || oldTableName.equals("wind_CCommodityFuturesEODPrices"))
            ret = ret && synchronizeAllData(oldTableName, newTableName, windColumns, startStr, endStr);
        else if(oldTableName.equals("wind_AShareDescription")) {
            ret = ret && synchronizeDataset(oldTableName, newTableName, windCodes, windColumns, startStr, endStr);
        }
        else if(oldTableName.equals("wind_AShareFinancialIndex")){
            assert false;
        }else if(oldTableName.equals("wind_GlobalMacrography_test")){
            assert false;
        }else{
            log.info("invalid oldTable name: {}", oldTableName);
            return false;
        }

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
    public boolean synchronizeOneDayData(String oldTableName, String newTableName, String windColumn, Date date, List<String> existingWindCodes) throws Exception{
        String oldDbColumn = metadataService.windColumn2DbColumn(oldTableName, windColumn);
        String newDbColumn = metadataService.windColumn2DbColumn(newTableName, windColumn);
        String dateStr = simpleDateFormat.format(date);
        List<MetadataDetail> metadataDetails = metadataService.queryMetadataDetails(oldTableName);
//        String tradeDt = null;
//        for(MetadataDetail metadataDetail: metadataDetails){
//            //日行情表
//            if(metadataDetail.getDbColumn().equals("trade_dt")){
//                tradeDt = metadataDetail.getDbColumn();
//            }
//        }
        if(oldTableName.equals("wind_AShareEODPrices_test") || oldTableName.equals("wind_CCommidityFuturesEODPrices_test")){
            //String dateDbColumn = metadataService.getTradeDtForDbColumn(oldTableName);
            boolean status = true, flag = false;
            //Set<String> existingWindCodes = Utils.getExistingWindCode(oldTableName, dateDbColumn, dateStr);
            for(String windCode: existingWindCodes){
                String value = Utils.getData(oldTableName, windCode, dateStr, oldDbColumn);
                // cache 中没有数据，从wind拿
                if(value == null){
                    value = dataService.getDataFromWind(windCode, dateStr, windColumn);
                    // wind 中没有
                    if(value == null){
                        log.info("pull date from wind fail. there is no data in this day");
                        //throw new ApiException("pull date from wind fail.");
                    }else{
                        // wind 中有数据将该数据先写入oldTable，再写入newTable
                        status = status && dataService.updateData(oldTableName, windCode, dateStr, oldDbColumn, value);
                    }
                    // 然后

                }
                // 更新newTable
                status = status && dataService.updateData(newTableName, windCode, dateStr, newDbColumn, value);

            }
            return status;
        }else{
            // TODO
            return false;
        }
    }

    @Override
    public boolean synchronizeTimeRangeData(String oldTableName, String newTableName, String windColumn, String startStr, String endStr, List<String> existingWindCodes) throws Exception {
        Date startDate = simpleDateFormat.parse(startStr), endDate = simpleDateFormat.parse(endStr);
        Calendar c = Calendar.getInstance();
        c.setTime(endDate);
        c.add(Calendar.DATE, 1);
        endDate = c.getTime();
        c.clear();

        boolean status = true;
        for(Date cur = startDate; !cur.equals(endDate); cur = c.getTime()){
            log.info("synchronize date: {}", cur);
            status = status && synchronizeOneDayData(oldTableName, newTableName, windColumn, cur, existingWindCodes);
            c.setTime(cur);
            c.add(  Calendar.DATE, 1);
        }
        return status;
    }

    /**
     * 用于行情表的数据同步
     * @param oldTableName 旧表名
     * @param newTableName 用户新建的数据表的名字
     * @param windColumns 用户想要的wind列
     * @param startStr 起始时间
     * @param endStr 终止时间
     * @return Boolean
     * @throws Exception ？
     */
    @Override
    public boolean synchronizeAllData(String oldTableName, String newTableName, List<String> windColumns, String startStr, String endStr) throws Exception{
        boolean status = true;
        String dateDbColumn = metadataService.getTradeDtForDbColumn(oldTableName);
        List<String> existingWindCodes = Utils.getExistingWindCodes(oldTableName, dateDbColumn, startStr, endStr);
        for(String windColumn: windColumns){
            if(!windColumn.equals("windcode"))
                status = status && synchronizeTimeRangeData(oldTableName, newTableName, windColumn, startStr, endStr, existingWindCodes);
        }
        return status;
    }

    /**
     * 用户数据集的同步
     * @param oldTableName 旧表名
     * @param newTableName 用户新建的数据表的名字
     * @param windColumns 用户想要的wind列
     * @return Boolean
     * @throws Exception ？
     */
    @Override
    public boolean synchronizeDataset(String oldTableName, String newTableName, List<String> windCodes, List<String> windColumns, String startStr, String endStr) throws Exception {
        boolean status = true;
        for(String windCode: windCodes){
            for(String windColumn: windColumns){
                if(!windColumn.equals("windcode")){
                    String oldDbColumn = metadataService.windColumn2DbColumn(oldTableName, windColumn);
                    String newDbColumn = metadataService.windColumn2DbColumn(newTableName, windColumn);
                    String value = Utils.getData(oldTableName, windCode, oldDbColumn);
                    // 存在该行，但对应列的值为NULL
                    if(value == null){
                        // 和日行情表用同一个
                        value = dataService.getDataFromWind(windCode, endStr, windColumn);
                        // 先更新旧表的数据。如果没有该行则新加一行
                        status = status && dataService.updateData(oldTableName, windCode, oldDbColumn, value);
                    }
                    status = status && dataService.updateData(newTableName, windCode, newDbColumn, value);
                }
            }
        }
        return false;
    }
}
