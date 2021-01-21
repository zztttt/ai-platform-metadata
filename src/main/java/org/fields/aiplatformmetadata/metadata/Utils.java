package org.fields.aiplatformmetadata.metadata;

import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class Utils {
    private static String url = "jdbc:mysql://rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com:3306/wind?useSSL=false&characterEncoding=utf-8&serverTimezone=GMT%2B8&allowPublicKeyRetrieval=true";
    private static String username = "zzt";
    private static String password = "Zzt19980924x";
    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        }catch (Exception e){
            e.printStackTrace();
        }

    }
    public static boolean createTable(String tableName, List<String> columns, List<String> columnTypes) throws Exception{
        Connection conn = DriverManager.getConnection(url, username, password);
        Statement stat = conn.createStatement();
        String sql = "create table " + tableName + "(";
        sql = sql + "id " + "bigint(64) primary key not null auto_increment,";
        int len = columns.size();
        for(int i = 0; i < len - 1; ++i){
            sql = sql + columns.get(i) + " " + columnTypes.get(i) + " null, ";
        }
        sql = sql + columns.get(len - 1) + " " + columnTypes.get(len - 1) + "null )";
        //System.out.println(sql);
        stat.executeUpdate(sql);
        stat.close();
        conn.close();
        return true;
    }

    public static boolean insertIntoTable(String tableName, List<String> columns){
        Connection conn = null;
        Statement stat = null;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();
            String sql = "insert into " + tableName + "(";
            int len = columns.size();
            for(int i = 0; i < len - 1; ++i){
                sql = sql + columns.get(i) + ", ";
            }
            sql = sql + columns.get(len - 1) + " " + ") value (";
            for(int i = 0; i < len - 1; ++i){
                sql = sql + String.valueOf(i) + ", ";
            }
            sql = sql + String.valueOf(len) + ")";
            System.out.println(sql);
            stat.executeUpdate(sql);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                if(stat != null)
                    stat.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            try{
                if(conn != null)
                    conn.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            return true;
        }
    }

    public static boolean deleteTable(String tableName) throws Exception{
        Connection conn = DriverManager.getConnection(url, username, password);
        Statement stat = conn.createStatement();

        String sql = "drop table " + tableName;
        stat.executeUpdate(sql);
        stat.close();
        conn.close();
        return true;
    }

    public static boolean addNewColumn(String tableName, String column, String type) throws Exception{
        Connection conn = DriverManager.getConnection(url, username, password);
        Statement stat = conn.createStatement();

        String sql = "alter table " + tableName + " add column " + column + " " + type + " null";
        stat.executeUpdate(sql);
        stat.close();
        conn.close();
        return true;
    }

    public static Set<String> getExistingWindCode(String tableName, String codeDbColumn, String dateDbColumn, String dateStr) throws Exception{
        Connection conn = DriverManager.getConnection(url, username, password);
        Statement stat = conn.createStatement();

        String sql = "select " + codeDbColumn + " from " + tableName + " where " + dateDbColumn + " = " + dateStr;
        //String sql = "select * from metadata";
        System.out.println(sql);

        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery(sql);

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnSize = resultSetMetaData.getColumnCount();

        Set<String> windCodes = new HashSet<>();
        while(resultSet.next()){
//            for(int i = 1; i <= columnSize ; i++){
//                System.out.printf("%-12s", resultSet.getObject(i));
//            }
            String cur = (String) resultSet.getObject(1);//第1列
            if(!windCodes.contains(cur))
                windCodes.add(cur);
        }
        stat.close();
        conn.close();
        return windCodes;
    }

    public static String getData(String tableName, String windCode, String dateStr, String windColumn){
        Connection conn = null;
        Statement stat = null;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();

            String sql = SqlUtils.selectData(tableName, windCode, dateStr, windColumn);
            log.info("getData execute sql: {}", sql);
            ResultSet resultSet = stat.executeQuery(sql);
            String ret = null;
            while(resultSet.next()){
                ret = (String) resultSet.getObject(1);
            }
            return ret;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                if(stat != null)
                    stat.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            try{
                if(conn != null)
                    conn.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            return null;
        }
    }

    public static boolean updateData(String tableName, String windCode, String dateStr, String windColumn, String value){
        Connection conn = null;
        Statement stat = null;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();

            List<String> attributions = new ArrayList<String>(){{add(windColumn);}};
            List<String> values = new ArrayList<String>(){{add(value);}};
            String sql = SqlUtils.update(tableName, windCode, dateStr, attributions, values);

            log.info("updateData execute sql: {}", sql);
            int ret = stat.executeUpdate(sql);
            assert ret <= 1;
            return ret == 1? true:false;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                if(stat != null)
                    stat.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            try{
                if(conn != null)
                    conn.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            return false;
        }
    }

    public static boolean insertData(String tableName, String windCode, String dateStr, String windColumn, String value){
        Connection conn = null;
        Statement stat = null;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();

            List<String> attributions = new ArrayList<String>(){{add(windColumn);}};
            List<String> values = new ArrayList<String>(){{add(value);}};

            String sql = SqlUtils.insert(tableName, windCode, dateStr, attributions, values);
            log.info("insertData execute sql: {}", sql);
            int ret = stat.executeUpdate(sql);
            assert ret <= 1;
            return ret == 1? true:false;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                if(stat != null)
                    stat.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            try{
                if(conn != null)
                    conn.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            return false;
        }
    }

    public static boolean isLineExisting(String tableName, String windCode, String dateStr){
        Connection conn = null;
        Statement stat = null;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();

            String sql = SqlUtils.selectLine(tableName, windCode, dateStr);
            log.info("isLineExisting execute sql: {}", sql);
            ResultSet resultSet = stat.executeQuery(sql);
            boolean ret = false;
            while(resultSet.next()){
                ret = true;
            }
            return ret;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                if(stat != null)
                    stat.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            try{
                if(conn != null)
                    conn.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            return false;
        }
    }

    public static String callScript(String[] args){
        return null;
    }
}
