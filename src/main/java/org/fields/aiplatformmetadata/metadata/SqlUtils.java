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

    // 行情A股
    public static String insertNewLine(String tableName, String windCodeColumn, String dateStrColumn, String windCode, String dateStr){
        String sql = "insert into " + tableName + " ("
                + windCodeColumn + ", " + dateStrColumn + ") "
                + "value ('" + windCode + "', " + dateStr + ")";
        return  sql;
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

    // select WINDCOLUMN from table1 where s_info_windcode = windcode and trade_dt = 20190601
    public static String selectData(String tableName, String windcode, String dateStr, String windColumn){
        String sql = "select " + windColumn + " from " + tableName +
                " where s_info_windcode = '" + windcode + "' and trade_dt = " + dateStr;
        return sql;
    }
}
