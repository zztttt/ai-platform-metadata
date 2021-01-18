package org.fields.aiplatformmetadata;

import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CreateAndDeleteRootTablesTest {
    @Autowired
    TableService tableService;

    @Test
    public void Test(){
        Assert.assertTrue(tableService.initRootTables());
        System.out.println(123);
    }

    @After
    public void release(){
        Assert.assertTrue(tableService.deleteRootTables());
    }
}
