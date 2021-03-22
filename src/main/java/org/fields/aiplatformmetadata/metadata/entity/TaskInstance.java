package org.fields.aiplatformmetadata.metadata.entity;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@Slf4j
public class TaskInstance implements Runnable{
    @Autowired
    TableService tableService;

    private JSONObject parameter;

    public TaskInstance(String s){
        try{
            this.parameter = JSONObject.parseObject(s);
        }catch (Exception e){
            log.error("parse task parameter error.");
            e.printStackTrace();
            throw e;
        }
    }
    @Override
    public void run(){
        if(tableService == null){
            log.error("autowire table service error.");
            return;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DATE, -1);
        Date lastDate = c.getTime();
        String oldTableName = parameter.getString("oldTableName");
        String newTableName = parameter.getString("newTableName");
        String updateTime = simpleDateFormat.format(date);
        String updateUser = parameter.getString("updateUser");
        String startTime = simpleDateFormat.format(lastDate), endTime = simpleDateFormat.format(lastDate);
        JSONArray windCodesJson = parameter.getJSONArray("windCodes");
        JSONArray columns = parameter.getJSONArray("columns");
        List<String> windCodes = new ArrayList<>(), windColumns = new ArrayList<>(), userColumns = new ArrayList<>();
        for(Object windCode: windCodesJson){
            windCodes.add((String) windCode);
        }
        for(Object column: columns){
            JSONObject jsonObject = (JSONObject) column;
            windColumns.add(jsonObject.getString("windColumn"));
            userColumns.add(jsonObject.getString("userColumn"));
        }

        log.info("task {} is scheduled.", newTableName);
        try{
            tableService.updateTable(oldTableName, newTableName, updateTime, updateUser, startTime, endTime, windCodes, windColumns, userColumns);
            log.info("updateTable success. tableName:{}. startTime:{}, endTime:{}", newTableName, startTime, endTime);
        }catch (Exception e){
            log.error("updateTable error. tableName:{}. error message:{}", newTableName, e.getMessage());
            e.printStackTrace();
        }

    }
}
