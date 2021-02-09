package org.fields.aiplatformmetadata.metadata.service.Impl;

import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.exception.ApiException;
import org.fields.aiplatformmetadata.metadata.Utils;
import org.fields.aiplatformmetadata.metadata.service.DataService;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

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

    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");


    @Override
    public Boolean updateTable(String oldTableName, String newTableName, String updateTime, String updateUser, String startStr, String endStr, List<String> windCodes, List<String> windColumns) throws ApiException {
        if (!metadataService.isTableExisting(oldTableName)) {
            log.info("updateTable error: old table {} is not existing", oldTableName);
            throw new ApiException("createTable error: old table is not existing");
        }
        if (!metadataService.isTableExisting(newTableName)) {
            log.info("updateTable error: new table {} is not existing", newTableName);
            throw new ApiException("createTable error: new Table is already existing");
        }
        Boolean status = true;
        try {
            status = status && synchronizeCodes(oldTableName, newTableName, windCodes, startStr, endStr);
        }catch (ApiException e){
            throw e;
        }finally {
            return status;
        }
    }

    @Override
    public Boolean createTable(String oldTableName, String newTableName, String updateTime, String updateUser, String startStr, String endStr, List<String> windCodes, List<String> windColumns, List<String> userColumns) throws Exception{
        if (!metadataService.isTableExisting(oldTableName)) {
            log.info("createTable error: old table {} is not existing", oldTableName);
            throw new ApiException("createTable error: old table is not existing");
        }
        if (metadataService.isTableExisting(newTableName)) {
            log.info("createTable error: new table {} is already existing", newTableName);
            throw new ApiException("createTable error: new Table is already existing");
        }
        String functionName = metadataService.getFunctionName(oldTableName);
        List<String> types = new ArrayList<>();
        List<String> dbColumns = new ArrayList<>();
        for (String windColumn : windColumns) {
            String type = metadataService.getType(oldTableName, windColumn);
            types.add(type);
            dbColumns.add(metadataService.windColumn2DbColumn(oldTableName, windColumn));
        }
        List<Map<String, String>> metadataDetails = new ArrayList<>();
        int len = windColumns.size();
        for (int i = 0; i < len; ++i) {
            String userColumn = userColumns.get(i), windColumn = windColumns.get(i), dbColumn = dbColumns.get(i), type = types.get(i);
            // add the metadataDetail
            metadataDetails.add(new HashMap<String, String>() {{
                put("tableName", newTableName);
                put("windColumn", windColumn);
                put("dbColumn", dbColumn);
                put("userColumn", userColumn);
                put("type", type);
            }});
        }
        try{
            utils.createTable(newTableName, dbColumns, types);
            metadataService.insertTableMetadata(newTableName, functionName, updateTime, updateUser);
            metadataService.insertTableMetadataDetail(metadataDetails);
            synchronizeCodes(oldTableName, newTableName, windCodes, startStr, endStr);
            return true;
        }catch (Exception e){
            throw e;
        }
    }

    @Override
    public Boolean synchronizeCodes(String oldTableName, String newTableName, List<String> windCodes, String startStr, String endStr) throws Exception{
        try{
            for(String windCode: windCodes){
                synchronizeCode(oldTableName, newTableName, windCode, startStr, endStr);
            }
            return true;
        }catch (Exception e){
            throw e;
        }
    }

    @Override
    public Boolean synchronizeCode(String oldTableName, String newTableName, String windCode, String startStr, String endStr) throws Exception{
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
            //Map<String, List<String>> emptyCell = new HashMap<>();
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
                    List<String> nonEmptyColumn = new ArrayList<>();
                    List<Object> oldValues = new ArrayList<>();
                    for(Map.Entry<String, Object> entry: map.entrySet()){
                        if(entry.getKey().equals("id"))
                            continue;
                        //log.info("pair: {}", entry);
                        String windColumn = metadataService.dbColumn2windColumn(oldTableName, entry.getKey());
                        if(metadataService.isColumnExist(newTableName, windColumn)){
                            if(entry.getValue() == null){
                                emptyColumn.add(windColumn);
                            }else{
                                nonEmptyColumn.add(windColumn);
                                oldValues.add(entry.getValue());
                            }
                        }
                    }
                    // pull old data from cache, s_info_code 和 trade_dt 这两列当主键来用的
                    status = status && utils.insertOneLine(newTableName, windCode, dateStr);
                    status = status &&  utils.updateOneLine(newTableName, windCode, dateStr, nonEmptyColumn, oldValues);
                    // pull new data
                    List<Object> values = new ArrayList<>();
                    for(String windColumn: emptyColumn){
                        String[] args = new String[3];
                        args[0]=windCode;args[1]=dateStr;args[2]=windColumn;
                        String value = utils.callScript(args);
                        //String value = "12.34";
                        values.add(value);
                    }
                    // remove None value index
                    List<Integer> index = new ArrayList<>();
                    int len = emptyColumn.size();
                    for(int i = 0; i < len; ++i){
                        if(values.get(i).equals("None")){
                            index.add(i);
                        }
                    }
                    for(int i: index){
                        emptyColumn.remove(i);
                        values.remove(i);
                    }
                    if(emptyColumn.size() > 0){
                        utils.updateOneLine(newTableName, windCode, dateStr, emptyColumn, values);
                        utils.updateOneLine(oldTableName, windCode, dateStr, emptyColumn, values);
                    }

                }else{
                    // cache 里没数据，整天都要从wind拿
                    log.info("cache miss");
                    // 先查wind 如果有这么一天再进行更新
                    String[] args = new String[3];
                    args[0] = windCode; args[1] = dateStr; args[2] = "windcode";
                    String ret = utils.callScript(args);
                    //String ret = null;
                    if(ret == null){
                        // 当天没有数据, skip
                    }else{
                        // 当天有数据
                        status = status && utils.insertOneLine(oldTableName, windCode, dateStr);
                        status = status && utils.insertOneLine(newTableName, windCode, dateStr);

                        List<String> windColumns = metadataService.queryWindColumnsOfTable(newTableName);
                        List<Object> values = new ArrayList<>();
                        for(String windColumn: windColumns){
                            String[] arg = new String[3];
                            arg[0]=windCode;arg[1]=dateStr;arg[2]=windColumn;
                            String value = null;
                            if(windColumn.equals("windcode")){
                                value = windCode;
                            }else if (windColumn.equals("lastradeday_s")){
                                value = dateStr;
                            }else{
                                value = utils.callScript(arg);
                            }
                            assert value != null;
                            values.add(value);
                        }
                        // remove None value index
                        List<Integer> index = new ArrayList<>();
                        int len = windColumns.size();
                        for(int i = 0; i < len; ++i){
                            if(values.get(i).equals("None")){
                                index.add(i);
                            }
                        }
                        for(int i: index){
                            windColumns.remove(i);
                            values.remove(i);
                        }
                        status = status && utils.updateOneLine(newTableName, windCode, dateStr, windColumns, values);
                        status = status && utils.updateOneLine(oldTableName, windCode, dateStr, windColumns, values);
                    }
                }
                c.setTime(cur);
                c.add(Calendar.DATE, 1);
            }
            return status;
        }catch (Exception e){
            throw e;
        }
    }
}
