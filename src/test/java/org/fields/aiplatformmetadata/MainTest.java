package org.fields.aiplatformmetadata;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MainTest {
    @Test
    public void test(){
        List<Object> list = new ArrayList<>();
        list.add("s");
        list.add(12.3);
        list.add(new ArrayList<>());
    }
}
