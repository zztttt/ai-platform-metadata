package org.fields.aiplatformmetadata;

import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SynchronizeDataTest {
    @Autowired
    TableService tableService;

    @Test
    public void test()throws Exception{
        tableService.synchronizeRangeData("", "", "", "20190601", "20190630");
    }
}
