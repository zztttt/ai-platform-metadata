package org.fields.aiplatformmetadata.metadata;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.common.Constant;
import org.springframework.util.Assert;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

@Slf4j
public class Utils {
    private static final String url = "jdbc:mysql://rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com:3306/wind?useSSL=false&characterEncoding=utf-8&serverTimezone=GMT%2B8&allowPublicKeyRetrieval=true";
    private static final String username = "zzt";
    private static final String password = "Zzt19980924x";
    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static boolean isTableExist(String tableName){
        String sql = "show tables like " +tableName;
        ResultSet resultSet = executeQuery(sql);
        boolean isExist = false;
        try{
            while(resultSet.next()){
                isExist = true;
                break;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            return isExist;
        }
    }
    public static ResultSet executeQuery(String sql){
        Connection conn = null;
        Statement stat = null;
        ResultSet resultSet = null;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();

            resultSet = stat.executeQuery(sql);
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
            return resultSet;
        }
    }
    public static int executeUpdate(String sql){
        Connection conn = null;
        Statement stat = null;
        int ret = 0;
        ResultSet resultSet = null;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();

            ret = stat.executeUpdate(sql);
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
            return ret;
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

    public static List<String> getExistingWindCodes(String tableName, String dateDbColumn, String startStr, String endStr) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date startDate = simpleDateFormat.parse(startStr), endDate = simpleDateFormat.parse(endStr);
        Calendar c = Calendar.getInstance();
        c.setTime(endDate);
        c.add(Calendar.DATE, 1);
        endDate = c.getTime();
        c.clear();

        List<String> windCodes = new ArrayList<>();
        for(Date cur = startDate; !cur.equals(endDate); cur = c.getTime()){
            String dateStr = simpleDateFormat.format(cur);
            getExistingWindCodes(windCodes, tableName, dateDbColumn, dateStr);
            c.setTime(cur);
            c.add(  Calendar.DATE, 1);
        }
        return windCodes;
    }

    private static void getExistingWindCodes(List<String> windCodes, String tableName, String dateDbColumn, String dateStr){
        String sql = SqlUtils.selectWindCode(tableName, dateDbColumn, dateStr);
        Connection conn = null;
        Statement stat = null;
        ResultSet resultSet = null;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();

            resultSet = stat.executeQuery(sql);
            while(resultSet.next()){
                String code = (String) resultSet.getObject(1);
                if(!windCodes.contains(code)){
                    windCodes.add(code);
                }
            }
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
            try{
                if(resultSet != null)
                    resultSet.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static Set<String> getExistingWindCode(String tableName, String dateDbColumn, String dateStr) throws Exception{
        Connection conn = DriverManager.getConnection(url, username, password);
        Statement stat = conn.createStatement();

        String sql = "select s_info_windcode from " + tableName + " where " + dateDbColumn + " = " + dateStr;
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

    /**
     * 行情表从mysql中获取数据
     * @param tableName
     * @param windCode
     * @param dateStr
     * @param dbColumn
     * @return
     */
    public static String getData(String tableName, String windCode, String dateStr, String dbColumn){
        Connection conn = null;
        Statement stat = null;
        String ret = null;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();

            String sql = SqlUtils.selectData(tableName, windCode, dateStr, dbColumn);
            log.info("getData execute sql: {}", sql);
            ResultSet resultSet = stat.executeQuery(sql);
            while(resultSet.next()){
                ret = (String) resultSet.getObject(1);
            }
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
            return ret;
        }
    }

    /**
     * 用于非行情表拿数据
     * @param tableName
     * @param windCode
     * @param dbColumn
     * @return
     */
    public static String getData(String tableName, String windCode, String dbColumn){
        String sql = SqlUtils.selectData(tableName, windCode, dbColumn);
        ResultSet resultSet = executeQuery(sql);
        String ret = null;
        try {
            while(resultSet.next()){
                ret = (String) resultSet.getObject(1);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            return ret;
        }
    }

    /**
     * 行情表
     * @param tableName
     * @param windCodeColumn
     * @param dateStrColumn
     * @param windCode
     * @param dateStr
     * @param windColumn
     * @param value
     * @return
     */
    public static boolean updateData(String tableName, String windCodeColumn, String dateStrColumn, String windCode, String dateStr, String windColumn, String value){
        Connection conn = null;
        Statement stat = null;
        int ret = 0;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();

            List<String> attributions = new ArrayList<String>(){{add(windColumn);}};
            List<String> values = new ArrayList<String>(){{add(value);}};
            String sql = SqlUtils.update(tableName, windCodeColumn, dateStrColumn, windCode, dateStr, attributions, values);

            log.info("updateData execute sql: {}", sql);
            ret = stat.executeUpdate(sql);
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
            assert ret <= 1;
            return ret == 1;
        }
    }
    public static boolean updateData(String tableName, String windCodeColumn, String windCode, String dbColumn, String value){
        String sql = SqlUtils.update(tableName, windCodeColumn, windCode, dbColumn, value);
        int ret = executeUpdate(sql);
        assert ret <= 1;
        return ret == 1;
    }

    /**
     * 行情表新建一行
     * @param tableName
     * @param windCodeColumn
     * @param dateStrColumn
     * @param windCode
     * @param dateStr
     * @return
     */
    public static boolean insertNewLine(String tableName, String windCodeColumn, String dateStrColumn, String windCode, String dateStr){
        String sql = SqlUtils.insertNewLine(tableName, windCodeColumn, dateStrColumn, windCode, dateStr);
        int ret = executeUpdate(sql);
        return ret == 1;
    }

    /**
     * dataset
     * @param tableName
     * @param windCodeColumn
     * @param windCode
     * @return
     */
    public static boolean insertNewLine(String tableName, String windCodeColumn, String windCode){
        String sql = SqlUtils.insertNewLine(tableName, windCodeColumn, windCode);
        int ret = executeUpdate(sql);
        return ret == 1;
    }

    public static boolean insertData(String tableName, String windCode, String dateStr, String windColumn, String value){
        Connection conn = null;
        Statement stat = null;
        int ret = 0;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();

            List<String> attributions = new ArrayList<String>(){{add(windColumn);}};
            List<String> values = new ArrayList<String>(){{add(value);}};

            String sql = SqlUtils.insert(tableName, windCode, dateStr, attributions, values);
            log.info("insertData execute sql: {}", sql);
            ret = stat.executeUpdate(sql);
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
            assert ret <= 1;
            return ret == 1? true:false;
        }
    }

    // 行情表
    public static boolean isLineExisting(String tableName, String windDbColumn, String dateDbColumn, String windCode, String dateStr){
        Connection conn = null;
        Statement stat = null;
        boolean ret = false;
        try{
            conn = DriverManager.getConnection(url, username, password);
            stat = conn.createStatement();
            String sql = SqlUtils.selectLine(tableName, windDbColumn, dateDbColumn, windCode, dateStr);
            log.info("isLineExisting execute sql: {}", sql);
            ResultSet resultSet = stat.executeQuery(sql);

            while(resultSet.next()){
                ret = true;
            }
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
            return ret;
        }
    }

    public static boolean isLineExisting(String tableName, String windDbColumn, String windCode){
        String sql = SqlUtils.selectLine(tableName, windDbColumn, windCode);
        ResultSet resultSet = executeQuery(sql);
        boolean ret = false;
        try{
            while(resultSet.next()){
                ret = true;
                break;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            return ret;
        }
    }

    // windcode datestr windcolumn
    public static String callScript(String[] args){
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
