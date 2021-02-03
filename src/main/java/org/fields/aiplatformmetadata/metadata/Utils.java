package org.fields.aiplatformmetadata.metadata;

import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.common.Constant;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

    public Boolean isLineExisting(String tableName, String windCodeDbColumn, String dateDbColumn, String windCode, String dateStr){
        String sql = sqlUtils.isLineExisting(tableName, windCodeDbColumn, dateDbColumn, windCode, dateStr);
        log.info("isLineExisting: {}", sql);
        List<Map<String, Object>> result = jdbcTemplate.queryForList(sql);
        return result.size() > 0;
    }

    public Boolean createTable(String tableName, List<String> columns, List<String> columnsTypes){
        // create table
        String sql = sqlUtils.createTable(tableName, columns, columnsTypes);
        jdbcTemplate.execute(sql);
        return true;
    }

    public List<Map<String, Object>> queryOneLine(String tableName, String windCodeDbColumn, String dateDbColumn, String windCode, String dateStr){
        String sql = sqlUtils.queryOneLine(tableName, windCodeDbColumn, dateDbColumn, windCode, dateStr);
        return jdbcTemplate.queryForList(sql);
    }

    public String queryOneCell(String tableName, String windCode, String dateStr, String windColumn){
        String windCodeDbColumn = metadataService.getWindCodeForDbColumn(tableName);
        String dateDbColumn = metadataService.getTradeDtForDbColumn(tableName);
        String sql = sqlUtils.queryOneCell(tableName, windCodeDbColumn, dateDbColumn, windCode, dateStr, windColumn);
        List<String> value = jdbcTemplate.queryForList(sql, String.class);
        return value.get(0);
    }

    public Boolean insertOneLine(String tableName, String windCode, String dateStr){
        String windCodeDbColumn = metadataService.getWindCodeForDbColumn(tableName);
        String dateDbColumn = metadataService.getTradeDtForDbColumn(tableName);
        String sql = sqlUtils.insertOneLine(tableName, windCodeDbColumn, dateDbColumn, windCode, dateStr);
        int ret = jdbcTemplate.update(sql);
        return ret == 1;
    }

    public Boolean updateOneLine(String tableName, String windCode, String dateStr, List<String> windColumns, List<Object> values){
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

    public String callScript(String[] args){
        String ret = null;
        try{
            ProcessBuilder pb = new ProcessBuilder().command("python", "-u", Constant.scriptPath, args[0], args[1], args[2]);
            Process p = pb.start();

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = null;
            while((line = bufferedReader.readLine()) != null){
                if(line.length() <= 5)
                    continue;
                //data: 12.33
                String tmp = line.substring(0, 4);
                if(tmp.equals("data")){
                    ret = line.substring(6, line.length());
                }
            }
            long result = p.waitFor();
            System.out.println("Process exit with:" + result);
            Assert.isTrue(result == 0, "script execution failed");
            bufferedReader.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return ret;
    }
}
