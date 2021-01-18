package org.fields.aiplatformmetadata;

import org.fields.aiplatformmetadata.metadata.Utils;
import org.junit.After;
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
public class UtilsTest {
    private final static String tableName = "testtable";
    private final static List<String> columns = new ArrayList<String>(){{add("column1");}};
    private final static List<String> types = new ArrayList<String>(){{add("varchar(1)");}};

    @Test
    public void step1() throws Exception{
        Utils.createTable(tableName, columns, types);
    }

    @Test
    public void step2() throws Exception{
        Utils.addNewColumn(tableName, "column2", "varchar(2)");
    }

    @Test
    public void step3() throws Exception{
        Utils.addNewColumn(tableName, "column3", "varchar(3)");
    }

    @AfterClass
    public static void release() throws Exception{
        Utils.deleteTable(tableName);
    }
}
