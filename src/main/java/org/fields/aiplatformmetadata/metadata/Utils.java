package org.fields.aiplatformmetadata.metadata;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

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

    public Boolean isTableExisting(String tableName){
        return sqlUtils.isTableExisting(tableName);
    }

    public Boolean isLineExisting(String tableName, String windCodeDbColumn, String dateDbColumn, String windCode, String dateStr){
        String sql = sqlUtils.isLineExisting(tableName, windCodeDbColumn, dateDbColumn, windCode, dateStr);
        log.info("isLineExisting: {}", sql);
        List<String> result = jdbcTemplate.queryForList(sql, String.class);
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
}
