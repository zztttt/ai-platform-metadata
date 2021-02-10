package org.fields.aiplatformmetadata.metadata;

import com.baomidou.mybatisplus.extension.exceptions.ApiException;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

@Slf4j
@Component
public class SqlUtils {
    @Autowired
    JdbcTemplate jdbcTemplate;

    public Boolean isTableExisting(String tableName){
        String sql = "show tables";
        List<String> tables = jdbcTemplate.queryForList(sql, String.class);
        for(String table: tables){
            if(table.toLowerCase(Locale.ROOT).equals(tableName.toLowerCase(Locale.ROOT)))
                return true;
        }
        return false;
    }

    // 带时间序列
    public String isLineExisting(String tableName, String windCodeColumn, String dateColumn, String windCode, String dateStr){
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tableName).append(" where ")
                .append(windCodeColumn).append("='").append(windCode).append("' and ")
                .append(dateColumn).append("='").append(dateStr).append("'");
        return sb.toString();
    }
    // 不带时间序列
    public String isLineExisting(String tableName, String windCodeColumn, String windCode){
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tableName).append(" where ").append(windCodeColumn).append("='").append(windCode).append("'");
        return sb.toString();
    }
    /**
     * create table
     * @return sql
     */
    public String createTable(String tableName, List<String> columns, List<String> columnTypes) throws Exception{
        if(isTableExisting(tableName)){
            log.info("table: {} is already existing", tableName);
            throw new ApiException("table is already existing");
        }
        if(columns.size()!= columnTypes.size()){
            log.info("createTable {}: columns and types size don't match, ", tableName);
            throw new ApiException("createTable columns and types size don't match");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("create table ").append(tableName).append("( id bigint(64) primary key not null auto_increment,");
        int len = columns.size();
        String link = "";
        for(int i = 0; i < len; ++i){
            sb.append(link).append(columns.get(i)).append(" ").append(columnTypes.get(i)).append(" NULL");
            link = ",";
        }
        sb.append(")");
        return sb.toString();
    }
    /**
     * insert into t1 (c1, c2...) value (v1, v2...)
     * @param tableName
     * @param columns
     * @param values
     * @return sql
     */
    public String insertOneLine(String tableName, List<String> columns, List<String> values) throws Exception{
        if(columns.size() != values.size()){
            log.info("insertNewLine columns and types size don't match");
            throw new ApiException("insertNewLine columns and types size don't match");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tableName).append("(");
        String link = "";
        int len = columns.size();
        for(int i = 0;i < len; ++i){
            sb.append(link).append(columns.get(i));
            link = ",";
        }
        sb.append(") value (");
        link = "";
        for(int i = 0; i < len; ++i){
            sb.append(link).append("'").append(values.get(i)).append("'");
            link = ",";
        }
        sb.append(")");
        return sb.toString();
    }

    // 带时间序列，新建一个空行
    public String insertOneLine(String tableName, String windCodeDbColumn, String dateDbColumn, String windCode, String dateStr){
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tableName).append("(").append(windCodeDbColumn).append(",").append(dateDbColumn)
                .append(") value (").append("'").append(windCode).append("','").append(dateStr).append("')");
        return sb.toString();
    }
    // 不带时间序列，新建一个空行
    public String insertOneLine(String tableName, String windCodeDbColumn, String windCode){
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tableName).append("(").append(windCodeDbColumn).append(") value (").append("'").append(windCode).append("')");
        return sb.toString();
    }

    // 带时间序列
    public String updateOneLine(String tableName, String windCodeDbColumn, String dateDbColumn, String windCode, String dateStr, List<String> dbColumns, List<Object> values){
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(tableName).append(" set ");
        String link = "";
        int len = dbColumns.size();
        for(int i = 0; i < len; ++i){
            sb.append(link).append(dbColumns.get(i)).append("='").append(values.get(i)).append("'");
            link = ",";
        }
        sb.append(" where ").append(windCodeDbColumn).append("='").append(windCode).append("' and ")
                .append(dateDbColumn).append("='").append(dateStr).append("'");
        return sb.toString();
    }
    // 不带时间序列
    public String updateOneLine(String tableName, String windCodeDbColumn, String windCode, List<String> dbColumns, List<Object> values) throws Exception{
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(tableName).append(" set ");
        String link = "";
        int len = dbColumns.size();
        for(int i = 0; i < len; ++i){
            sb.append(link).append(dbColumns.get(i)).append("='").append(values.get(i)).append("'");
            link = ",";
        }
        sb.append(" where ").append(windCodeDbColumn).append("='").append(windCode).append("'");
        return sb.toString();
    }

    /**
     * select * from t1 where windCodeDbColumn = 'windCode' and dateDbColumn = 'dateStr'
     * @param tableName
     * @param windCodeDbColumn
     * @param dateDbColumn
     * @param windCode
     * @param dateStr
     * @return
     */
    public String queryOneLine(String tableName, String windCodeDbColumn, String dateDbColumn, String windCode, String dateStr){
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tableName).append(" where ")
                .append(windCodeDbColumn).append("='").append(windCode).append("' and ")
                .append(dateDbColumn).append("='").append(dateStr).append("'");
        return sb.toString();
    }

    /**
     * selct * from t1 where windCodeDbColumn = 'windCode'
     * @param tableName
     * @param windCodeDbColumn
     * @param windCode
     * @return
     */
    public String queryOneLine(String tableName, String windCodeDbColumn, String windCode){
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tableName).append(" where ").append(windCodeDbColumn).append("='").append(windCode).append("'");
        return sb.toString();
    }

    public String queryOneCell(String tableName, String windCodeDbColumn, String dateDbColumn, String windCode, String dateStr, String windColmn){
        StringBuilder sb = new StringBuilder();
        sb.append("select ").append(windCode).append(" from ").append(tableName).append(" where ")
                .append(windCodeDbColumn).append("='").append(windCode).append("' and ")
                .append(dateDbColumn).append("='").append(dateStr).append("'");
        return sb.toString();
    }
}
