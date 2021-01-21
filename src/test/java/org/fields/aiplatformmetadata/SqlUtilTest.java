package org.fields.aiplatformmetadata;


import org.fields.aiplatformmetadata.metadata.SqlUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SqlUtilTest {
    private String tableName = "table1";
    private String code = "code1";
    private String date = "20190603";
    @Test
    public void selectTest(){
        List<String> attributions = new ArrayList<String>(){{
            add("s_dq_preclose");
            add("s_dq_open");
            add("s_dq_avgprice");
        }};
        String sql = SqlUtils.select(tableName, code, date, attributions);
        System.out.println(sql);
    }
    @Test
    public void updateTest(){
        List<String> attributions = new ArrayList<String>(){{
            add("s_dq_preclose");
            add("s_dq_open");
            add("s_dq_avgprice");
        }};
        List<String> values = new ArrayList<String>(){{
           add("value1");
           add("value2");
           add("value3");
        }};
        String sql = SqlUtils.update(tableName, code, date, attributions, values);
        System.out.println(sql);
    }

    @Test
    public void insertTest(){
        List<String> attributions = new ArrayList<String>(){{
            add("s_dq_preclose");
            add("s_dq_open");
            add("s_dq_avgprice");
        }};
        List<String> values = new ArrayList<String>(){{
            add("value1");
            add("value2");
            add("value3");
        }};
        String sql = SqlUtils.insert(tableName, code, date, attributions, values);
        System.out.println(sql);
    }

    @Test
    public void selectLineTest(){
        String sql = SqlUtils.selectLine(tableName, "windcode", "20190601");
        System.out.println(sql);
    }
}
