package org.fields.aiplatformmetadata;

import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SynchronizeDataTest {
    @Autowired
    TableService tableService;

    private String oldTableName = "wind_AShareEODPrices_test";
    private String newTableName = "synchronize_table";
    private ArrayList<String> windColumns = new ArrayList<String>(){{
        add("windcode");
        add("lastradeday_s");
        add("pre_close");
        add("open");
    }};
    private String startStr = "20190601";
    private String endStr = "20190603";

    @Test
    public void test()throws Exception{
        tableService.synchronizeTimeRangeData(oldTableName, newTableName, "", startStr, endStr);
    }
}
