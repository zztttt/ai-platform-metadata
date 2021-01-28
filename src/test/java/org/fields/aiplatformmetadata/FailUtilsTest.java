package org.fields.aiplatformmetadata;

import org.fields.aiplatformmetadata.metadata.FailUtils;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(SpringRunner.class)
@SpringBootTest
public class FailUtilsTest {
    private final static String tableName = "testtable";
    private final static List<String> columns = new ArrayList<String>(){{add("column1");}};
    private final static List<String> types = new ArrayList<String>(){{add("varchar(1)");}};

    @Test
    public void step1() throws Exception{
        FailUtils.createTable(tableName, columns, types);
    }

    @Test
    public void step2() throws Exception{
        FailUtils.addNewColumn(tableName, "column2", "varchar(2)");
    }

    @Test
    public void step3() throws Exception{
        FailUtils.addNewColumn(tableName, "column3", "varchar(3)");
    }

    @Test
    public void step4() throws Exception{
        FailUtils.getExistingWindCode("wind_ashareeodprices_test", "trade_dt", "20190601");
    }

    @AfterClass
    public static void release() throws Exception{
        //Utils.deleteTable(tableName);
    }
}
