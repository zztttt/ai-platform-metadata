package org.fields.aiplatformmetadata.metadata.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.common.RespResult;
import org.fields.aiplatformmetadata.exception.ApiException;
import org.fields.aiplatformmetadata.metadata.entity.RequestDemo;
import org.fields.aiplatformmetadata.metadata.entity.TaskInstance;
import org.fields.aiplatformmetadata.metadata.entity.request.*;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.fields.aiplatformmetadata.metadata.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@RestController
@Component
public class TableController {
    @Autowired
    MetadataService metadataService;
    @Autowired
    TableService tableService;
    @Autowired
    TaskService taskService;
    @Autowired
    ThreadPoolTaskScheduler threadPoolTaskScheduler;
    @Autowired
    Map<Long, ScheduledFuture<?>> taskPool;
    @Autowired
    AutowireCapableBeanFactory autowireCapableBeanFactory;

    @PostMapping("/create")
    public RespResult create(@RequestBody CreateTable createTable){
        log.info("receive: {}", createTable);
        if(createTable.getOldTableName() == null){
            return RespResult.fail(500L, "oldTableName is null");
        }
        if(createTable.getNewTableName() == null){
            return RespResult.fail(500L, "newTableName is null");
        }
        if(createTable.getUpdateTime() == null){
            return RespResult.fail(500L,"updateTime is null");
        }
        if(createTable.getUpdateUser() == null){
            return RespResult.fail(500L,"updateUser is null");
        }
        if(createTable.getTimeRange() == null){
            return RespResult.fail(500L,"time range is null");
        }
        if(createTable.getWindCodes() == null){
            return RespResult.fail(500L,"windCodes is null");
        }
        if(createTable.getColumns() == null){
            return RespResult.fail(500L,"columns is null");
        }
        List<Column> columns = createTable.getColumns();
        List<String> windColumns = new ArrayList<>();
        //List<String> dbColumns = new ArrayList<>();
        List<String> userColumns = new ArrayList<>();
        for(Column column: columns){
            windColumns.add(column.getWindColumn());
            //dbColumns.add(column.getDbColumn());
            userColumns.add(column.getUserColumn());
        }
        try{
            Boolean status = tableService.createTable(
                    createTable.getOldTableName(),
                    createTable.getNewTableName(),
                    createTable.getUpdateTime(),
                    createTable.getUpdateUser(),
                    createTable.getTimeRange().get(0),
                    createTable.getTimeRange().get(1),
                    createTable.getWindCodes(),
                    windColumns,
                    userColumns);
            String type = createTable.getType();
            if(type.equals("实时")){
                String parameter = JSONObject.toJSONString(createTable);
                TaskInstance taskInstance = new TaskInstance(parameter);
                autowireCapableBeanFactory.autowireBean(taskInstance);
                ScheduledFuture<?> future = threadPoolTaskScheduler.schedule(taskInstance, new CronTrigger("0 0 1 * * ?")); // 每天凌晨一点执行一次
                status = status && taskService.insertNewTask(createTable.getUpdateUser(), createTable.getNewTableName(), "", parameter, "running");
                Long id = taskService.queryTaskId(createTable.getNewTableName());
                if(taskPool.containsKey(id)){
                    log.error("taskPool has already contains key:{}", id);
                }else{
                    taskPool.put(id, future);
                }
                if(status){
                    log.info("taskPool size increase. current running task count: {}. current added task id: {}", taskPool.size(), id);
                    return status? RespResult.success(""): RespResult.fail(500L, "taskPool increase error");
                }else{
                    log.error("taskPool increase error.");
                    return RespResult.fail(500L, "taskPool increase error");
                }
            }else if(type.equals("手动")){
                return status? RespResult.success(""): RespResult.fail(500L, "task execute error");
            }else{
                return RespResult.fail(500L, "type is invalid.");
            }

        } catch (Exception e){
            log.error("create error. message: {}", e.getMessage());
            return RespResult.fail(500L, e.getMessage());
        }

    }

    @PostMapping("/getWindTableName")
    public RespResult getWindTableName(@RequestBody GetWindTableName getWindTableName){
        log.info("getWindTableName: {}", getWindTableName);
        if(getWindTableName.getWindDes() == null){
            return RespResult.fail(500L, "getWindTableName para missing");
        }
        String windDes = getWindTableName.getWindDes();
        JSONArray data = new JSONArray();
        if(windDes.equals("wsd")){
            data.add("wind_AShareEODPrices_test");
            data.add("wind_CCommodity_test");
            data.add("wind_AShareDescription_test");
        }else if(windDes.equals("edb")){
            data.add("wind_GlobalMacrography_test");
        }else{
            return RespResult.fail(500L, "windDes error." + windDes);
        }
        return RespResult.success(data);
    }

    @PostMapping("/getWindTableDetails")
    public RespResult getWindTableDetails(@RequestBody GetWindTableDetails getWindTableDetails){
        log.info("getWindTableDetails: {}", getWindTableDetails);
        String windTableName = getWindTableDetails.getWindTableName();
        if(windTableName == null){
            return RespResult.fail(500L, "windTableName is null");
        }
        JSONArray ret = metadataService.getWindTableDetails(windTableName);
        log.info(ret.toString());
        return RespResult.success(ret);
    }

    @PostMapping("/update")
    public RespResult update(@RequestBody UpdateTable updateTable){
        log.info("update: {}", updateTable);
        if(updateTable.getOldTableName() == null){
            return RespResult.fail(500L, "oldTableName is null");
        }
        if(updateTable.getNewTableName() == null){
            return RespResult.fail(500L, "newTableName is null");
        }
        List<UpdateColumn> updateColumns = updateTable.getColumns();
        List<String> windColumns = new ArrayList<>();
        List<String> userColumns = new ArrayList<>();
        for(UpdateColumn updateColumn: updateColumns){
            windColumns.add(updateColumn.getWindColumn());
            userColumns.add(updateColumn.getUserColumn());
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
                    windColumns,
                    userColumns);
            return status? RespResult.success(""): RespResult.fail();
        }catch (Exception e){
            String error = e.getMessage();
            return RespResult.fail(400L, error);
        }
    }
}
