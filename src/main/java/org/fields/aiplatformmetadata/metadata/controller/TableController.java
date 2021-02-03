package org.fields.aiplatformmetadata.metadata.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.common.RespResult;
import org.fields.aiplatformmetadata.exception.ApiException;
import org.fields.aiplatformmetadata.metadata.entity.RequestDemo;
import org.fields.aiplatformmetadata.metadata.entity.request.*;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.fields.aiplatformmetadata.metadata.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
public class TableController {
    @Autowired
    MetadataService metadataService;
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
    public RespResult create(@RequestBody CreateTable createTable){
        log.info("receive: {}", createTable);
        List<Column> columns = createTable.getColumns();
        List<String> windColumns = new ArrayList<>();
        List<String> dbColumns = new ArrayList<>();
        List<String> userColumns = new ArrayList<>();
        for(Column column: columns){
            windColumns.add(column.getWindColumn());
            dbColumns.add(column.getDbColumn());
            userColumns.add(column.getUserColumn());
        }
        Boolean status = tableService.createTable(
                createTable.getOldTableName(),
                createTable.getNewTableName(),
                createTable.getUpdateTime(),
                createTable.getUpdateUser(),
                createTable.getTimeRange().get(0),
                createTable.getTimeRange().get(1),
                createTable.getWindCodes(),
                windColumns,
                dbColumns,
                userColumns);
        return status? RespResult.success(""): RespResult.fail();
    }

    @PostMapping("/getWindTableDetails")
    public RespResult getWindTableDetails(@RequestBody GetWindTableDetails getWindTableDetails){
        log.info("getWindTableDetails: {}", getWindTableDetails);
        String windTableName = getWindTableDetails.getWindTableName();
        JSONArray ret = metadataService.getWindTableDetails(windTableName);
        return RespResult.success(ret);
    }

    @PostMapping("/update")
    public RespResult update(@RequestBody UpdateTable updateTable){
        log.info("update: {}", updateTable);
        List<UpdateColumn> updateColumns = updateTable.getColumns();
        List<String> windColumns = new ArrayList<>();
        for(UpdateColumn updateColumn: updateColumns){
            windColumns.add(updateColumn.getWindColumn());
        }
        Boolean status = true;
        try {
            status = status && tableService.updateTable(
                    updateTable.getOldTableName(),
                    updateTable.getNewTableName(),
                    updateTable.getUpdateTime(),
                    updateTable.getUpdateUser(),
                    updateTable.getTimeRange().get(0),
                    updateTable.getTimeRange().get(1),
                    updateTable.getWindCodes(),
                    windColumns);
            return status? RespResult.success(""): RespResult.fail();
        }catch (ApiException e){
            String error = e.getMessage();
            return RespResult.fail(400L, error);
        }
    }
}
