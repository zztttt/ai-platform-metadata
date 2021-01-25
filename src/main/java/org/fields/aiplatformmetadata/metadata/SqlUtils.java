package org.fields.aiplatformmetadata.metadata;

import java.util.List;
import java.util.Map;

public class SqlUtils {
    // select db1, db2, ....,  from t1 where code = c1 and date = s1
    public static String select(String tableName, String code, String date, List<String> attributions){
        String sql = "select ";
        String link = "";
        for(String attibution: attributions){
            sql += link + attibution;
            link = ", ";
        }
        sql += " from " + tableName + " where s_info_windcode = '" + code + "' and trade_dt = " + date;
        return sql;
    }

    // update t1 set c1 = c1, c2 = c2, ... where cn = cn and cm = cm
    public static String update(String tableName, String windCodeColumn, String dateStrColumn, String windCode, String date, List<String> attributions, List<String> values){
        assert attributions.size() == values.size();
        String sql = "update " + tableName + " set ";
        String link = "";
        int len = attributions.size();
        for(int i = 0; i < len; ++i){
            sql += link + attributions.get(i) + " = " + values.get(i);
            link = ",";
        }
        sql += " where " + windCodeColumn + " = '" + windCode + "' and " + dateStrColumn + " = " + date;
        return sql;
    }

    /**
     * dateset
     * @param tableName
     * @param windCodeColumn
     * @param windCode
     * @param windDbColumn
     * @param value
     * @return
     */
    public static String update(String tableName, String windCodeColumn, String windCode, String windDbColumn, String value){
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(tableName).append(" set ").append(windDbColumn).append(" = ").append(value)
                .append(" where ").append(windCodeColumn).append(" = '").append(windCode).append("'");
        return sb.toString();
    }

    // 行情A股
    public static String insertNewLine(String tableName, String windCodeColumn, String dateStrColumn, String windCode, String dateStr){
        String sql = "insert into " + tableName + " ("
                + windCodeColumn + ", " + dateStrColumn + ") "
                + "value ('" + windCode + "', " + dateStr + ")";
        return  sql;
    }

    /**
     * dataset
     * @param tableName
     * @param windCodeColumn
     * @param windCode
     * @return
     */
    public static String insertNewLine(String tableName, String windCodeColumn, String windCode){
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tableName).append(" (").append(windCodeColumn).append(")").append(" value ( '").append(windCode).append("')");
        return sb.toString();
    }

    // insert into t1 (c1, c2...) value (v1, v2...)
    public static String insert(String tableName, String windCode, String date, List<String> attributions, List<String> values){
        assert attributions.size() == values.size();
        String sql = "insert into " + tableName + " (";
        String link = "";
        for(String attribution: attributions){
            sql += link + attribution;
            link = ",";
        }
        sql += ") value (";
        link = "";
        for(String value: values){
            sql += link + value;
            link = ",";
        }
        sql += ")";
        return sql;
    }
    // select * from table1 where d1(s_info_windcode) = 'windcode' and d2(trade_dt) = 20190601
    public static String selectLine(String tableName, String windDbColumn, String dateDbColumn, String windcode, String dateStr){
        String sql = "select * from " + tableName + " where " + windDbColumn + " = '" +  windcode + "' and " + dateDbColumn + " = " + dateStr;
        return sql;
    }

    // select * from table1 where d1(s_info_code) = '000001.SZ'
    public static String selectLine(String tableName, String windDbColumn, String windCode){
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tableName).append(" where ").append(windDbColumn).append(" = '").append(windCode).append("'");
        return sb.toString();
    }

    // select WINDCOLUMN from table1 where s_info_windcode = windcode and trade_dt = 20190601
    public static String selectData(String tableName, String windcode, String dateStr, String windColumn){
        String sql = "select " + windColumn + " from " + tableName +
                " where s_info_windcode = '" + windcode + "' and trade_dt = " + dateStr;
        return sql;
    }

    public static String selectData(String tableName, String windCode, String dbColumn){
        StringBuilder sb = new StringBuilder();
        sb.append("select ").append(dbColumn).append(" from ").append(tableName).append(" where s_info_windcode = '").append(windCode).append("'");
        return sb.toString();
    }

    public static String selectWindCode(String tableName, String dateDbColumn, String dateStr) {
        StringBuilder sb = new StringBuilder();
        sb.append("select s_info_windcode from ").append(tableName).append(" where ").append(dateDbColumn).append(" = ").append(dateStr);
        return sb.toString();
    }
}
