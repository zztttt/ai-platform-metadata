package org.fields.aiplatformmetadata.metadata;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.common.Constant;
import org.fields.aiplatformmetadata.exception.ApiException;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class Utils {
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    SqlUtils sqlUtils;
    @Autowired
    MetadataService metadataService;

    public Boolean isTableExisting(String tableName){
        return sqlUtils.isTableExisting(tableName);
    }

    // 带时间序列
    public Boolean isLineExisting(String tableName, String windCodeDbColumn, String dateDbColumn, String windCode, String dateStr) throws Exception{
        String sql = sqlUtils.isLineExisting(tableName, windCodeDbColumn, dateDbColumn, windCode, dateStr);
        log.info("isLineExisting: {}", sql);
        List<Map<String, Object>> result = jdbcTemplate.queryForList(sql);
        if(result.size() > 1){
            throw new ApiException("is LineExisting error. more than one line");
        }
        return result.size() == 1;
    }
    // 不带时间序列
    public Boolean isLineExisting(String tableName, String windCodeDbColumn, String windCode) throws Exception{
        String sql = sqlUtils.isLineExisting(tableName, windCodeDbColumn, windCode);
        log.info("isLineExisting: {}", sql);
        List<Map<String, Object>> result = jdbcTemplate.queryForList(sql);
        if(result.size() > 1){
            throw new ApiException("is LineExisting error. more than one line");
        }
        return result.size() == 1;
    }

    public Boolean createTable(String tableName, List<String> columns, List<String> columnsTypes) throws Exception{
        // create table
        try{
            String sql = sqlUtils.createTable(tableName, columns, columnsTypes);
            jdbcTemplate.execute(sql);
            return true;
        }catch (Exception e){
            throw e;
        }
    }

    // 带时间序列
    public List<Map<String, Object>> queryOneLine(String tableName, String windCodeDbColumn, String dateDbColumn, String windCode, String dateStr) throws Exception{
        String sql = sqlUtils.queryOneLine(tableName, windCodeDbColumn, dateDbColumn, windCode, dateStr);
        return jdbcTemplate.queryForList(sql);
    }
    // 不带时间序列
    public List<Map<String, Object>> queryOneLine(String tableName, String windCodeDbColumn, String windCode) throws Exception{
        String sql = sqlUtils.queryOneLine(tableName, windCodeDbColumn, windCode);
        return jdbcTemplate.queryForList(sql);
    }

    public String queryOneCell(String tableName, String windCode, String dateStr, String windColumn){
        String windCodeDbColumn = metadataService.getWindCodeForDbColumn(tableName);
        String dateDbColumn = metadataService.getTradeDtForDbColumn(tableName);
        String sql = sqlUtils.queryOneCell(tableName, windCodeDbColumn, dateDbColumn, windCode, dateStr, windColumn);
        List<String> value = jdbcTemplate.queryForList(sql, String.class);
        return value.get(0);
    }

    // 带时间序列
    public Boolean insertOneLine(String tableName, String windCode, String dateStr) throws Exception{
        String windCodeDbColumn = metadataService.getWindCodeForDbColumn(tableName);
        String dateDbColumn = metadataService.getTradeDtForDbColumn(tableName);
        String sql = sqlUtils.insertOneLine(tableName, windCodeDbColumn, dateDbColumn, windCode, dateStr);
        int ret = jdbcTemplate.update(sql);
        return ret == 1;
    }
    // 不带时间序列
    public Boolean insertOneLine(String tableName, String windCode) throws Exception{
        String windCodeDbColumn = metadataService.getWindCodeForDbColumn(tableName);
        String sql = sqlUtils.insertOneLine(tableName, windCodeDbColumn, windCode);
        int ret = jdbcTemplate.update(sql);
        return ret == 1;
    }

    // 带时间序列
    public Boolean updateOneLine(String tableName, String windCode, String dateStr, List<String> windColumns, List<Object> values) throws Exception{
        String windCodeDbColumn = metadataService.getWindCodeForDbColumn(tableName);
        String dateDbColumn = metadataService.getTradeDtForDbColumn(tableName);
        List<String> dbColumns = new ArrayList<>();
        for(String windColumn: windColumns){
            dbColumns.add(metadataService.windColumn2DbColumn(tableName, windColumn));
        }
        String sql = sqlUtils.updateOneLine(tableName, windCodeDbColumn, dateDbColumn, windCode, dateStr, dbColumns, values);
        int ret = jdbcTemplate.update(sql);
        return ret == 1;
    }
    // 不带时间序列
    public Boolean updateOneLine(String tableName, String windCOde, List<String> windColumns, List<Object> values) throws Exception{
        String windCodeDbColumn = metadataService.getWindCodeForDbColumn(tableName);
        List<String> dbColumns = new ArrayList<>();
        for(String windColumn: windColumns){
            dbColumns.add(metadataService.windColumn2DbColumn(tableName, windColumn));
        }
        String sql = sqlUtils.updateOneLine(tableName, windCodeDbColumn, windCOde, dbColumns, values);
        int ret = jdbcTemplate.update(sql);
        return ret == 1;
    }

    public String callScript(String[] args) throws Exception{
        String ret = null;
        ProcessBuilder pb = new ProcessBuilder().command("python", "-u", Constant.scriptPath, args[0], args[1], args[2]);
        Process p = pb.start();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = null;
        while((line = bufferedReader.readLine()) != null){
            if(line.equals(""))
                continue;
            if(line.charAt(0) == '{'){
                JSONObject jsonObject = JSONObject.parseObject(line);
                String data = String.valueOf(jsonObject.get("data"));
                //log.info("json data:{}", data);
                ret = data;
            }
        }
        long result = p.waitFor();
        System.out.println("Process exit with:" + result);
        if(result != 0){
            throw new ApiException("script execute fail. args:" +  args[0] + ", " +  args[1] + ", " +  args[2]);
        }
        bufferedReader.close();
        return ret;
    }

    public  String getEncoding(String str) {
        String encode = "GB2312";
        try {
            if (str.equals(new String(str.getBytes(encode), encode))) { //判断是不是GB2312
                String s = encode;
                return s; //是的话，返回“GB2312“，以下代码同理
            }
        } catch (Exception exception) {
        }
        encode = "ISO-8859-1";
        try {
            if (str.equals(new String(str.getBytes(encode), encode))) { //判断是不是ISO-8859-1
                String s1 = encode;
                return s1;
            }
        } catch (Exception exception1) {
        }
        encode = "UTF-8";
        try {
            if (str.equals(new String(str.getBytes(encode), encode))) { //判断是不是UTF-8
                String s2 = encode;
                return s2;
            }
        } catch (Exception exception2) {
        }
        encode = "GBK";
        try {
            if (str.equals(new String(str.getBytes(encode), encode))) { //判断是不是GBK
                String s3 = encode;
                return s3;
            }
        } catch (Exception exception3) {
        }
        return ""; //如果都不是，说明输入的内容不属于常见的编码格式。
    }
}
