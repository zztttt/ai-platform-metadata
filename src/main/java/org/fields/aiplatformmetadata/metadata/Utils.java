package org.fields.aiplatformmetadata.metadata;

import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class Utils {
    public static boolean createTable(String tableName, List<String> columns, List<String> columnTypes) throws Exception{
        Class.forName("com.mysql.cj.jdbc.Driver");

        //一开始必须填一个已经存在的数据库
        String url = "jdbc:mysql://rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com:3306/wind?useSSL=false&characterEncoding=utf-8&serverTimezone=GMT%2B8&allowPublicKeyRetrieval=true";
        Connection conn = DriverManager.getConnection(url, "zzt", "Zzt19980924x");
        Statement stat = conn.createStatement();

        //创建表
        String sql = "create table " + tableName + "(";
        int len = columns.size();
        for(int i = 0; i < len - 1; ++i){
            sql = sql + columns.get(i) + " " + columnTypes.get(i) + ", ";
        }
        sql = sql + columns.get(len - 1) + " " + columnTypes.get(len - 1)
                + ")";
        stat.executeUpdate(sql);
        stat.close();
        conn.close();
        return true;
    }

    public static boolean deleteTable(String tableName) throws Exception{
        Class.forName("com.mysql.cj.jdbc.Driver");

        String url = "jdbc:mysql://rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com:3306/wind?useSSL=false&characterEncoding=utf-8&serverTimezone=GMT%2B8&allowPublicKeyRetrieval=true";
        Connection conn = DriverManager.getConnection(url, "zzt", "Zzt19980924x");
        Statement stat = conn.createStatement();

        String sql = "drop table " + tableName;
        stat.executeUpdate(sql);
        stat.close();
        conn.close();
        return true;
    }

    public static boolean addNewColumn(String tableName, String column, String type) throws Exception{
        Class.forName("com.mysql.cj.jdbc.Driver");

        String url = "jdbc:mysql://rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com:3306/wind?useSSL=false&characterEncoding=utf-8&serverTimezone=GMT%2B8&allowPublicKeyRetrieval=true";
        Connection conn = DriverManager.getConnection(url, "zzt", "Zzt19980924x");
        Statement stat = conn.createStatement();

        String sql = "alter table " + tableName + " add column " + column + " " + type + " null";
        stat.executeUpdate(sql);
        stat.close();
        conn.close();
        return true;
    }
}
