package org.fields.aiplatformmetadata.metadata.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.metadata.entity.RequestDemo;
import org.fields.aiplatformmetadata.metadata.entity.request.CreateTable;
import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.fields.aiplatformmetadata.metadata.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
public class TableController {
    @Autowired
    TableService tableService;
    @Autowired
    TaskService taskService;

    @GetMapping("test")
    public String test(){
        return "test";
    }
    @PostMapping("/testpost")
    public JSONObject create(@RequestBody RequestDemo requestDemo){
        System.out.println(requestDemo);
        List<String> list = requestDemo.getList();
        for(String s: list){
            System.out.println(s);
        }
        return null;
    }

    @PostMapping("/create")
    public JSONObject create(@RequestBody CreateTable createTable) throws Exception{
        log.info("receive: {}", createTable);
        JSONObject ret = new JSONObject();
        //tableService.checkAndInitRootTables();
        boolean status = tableService.createTable(
                createTable.getOldTableName(),
                createTable.getNewTableName(),
                createTable.getFunctionName(),
                createTable.getUpdateTime(),
                createTable.getUpdateUser(),
                createTable.getStartStr(),
                createTable.getEndStr(),
                createTable.getWindColumns(),
                createTable.getDbColumns(),
                createTable.getUserColumns(),
                createTable.getTypes());
        status = status && taskService.insertNewTask(createTable.getUpdateUser(), createTable.getNewTableName(), "无");
        ret.put("code", status?200:404);
        return ret;
    }
}
