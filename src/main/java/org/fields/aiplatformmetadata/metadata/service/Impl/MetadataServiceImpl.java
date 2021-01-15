package org.fields.aiplatformmetadata.metadata.service.Impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.metadata.entity.Metadata;
import org.fields.aiplatformmetadata.metadata.entity.MetadataDetail;
import org.fields.aiplatformmetadata.metadata.entity.Task;
import org.fields.aiplatformmetadata.metadata.mapper.MetadataDetailMapper;
import org.fields.aiplatformmetadata.metadata.mapper.MetadataMapper;
import org.fields.aiplatformmetadata.metadata.mapper.TaskMapper;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import lombok.*;

@Slf4j
@Service
public class MetadataServiceImpl implements MetadataService {
    @Autowired
    MetadataMapper metadataMapper;
    @Autowired
    MetadataDetailMapper metadataDetailMapper;
    @Autowired
    TaskMapper taskMapper;

    /**
     * @param tableName 表名
     * @param func 该表所用的函数
     * @return boolean
     */
    @Override
    public boolean insertTableMetadata(String tableName, String func, String updateTime, String updateUser){
        Metadata metadata = new Metadata();
        metadata.setTableName(tableName);
        metadata.setFunc(func);
        metadata.setUpdateTime(updateTime);
        metadata.setUpdateUser(updateUser);
        int ret = metadataMapper.insert(metadata);
        log.info("insertTableMetadata: {} line", ret);
        return ret == 1;
    }
    /**
     * @param set 包含对一个table的每个字段的具体值的说明。 eg tableName，windColumn， dbColumn, userColumn
     *        map 对MetadataDetail的的值进行说明
     * @return boolean
     */
    @Override
    public boolean insertTableMetadataDetail(Set<Map<String, String>> set){
        boolean status = true;
        for(Map<String, String> map: set){
            assert map.size() == 4 && map.containsKey("tableName") && map.containsKey("windColumn")
                    && map.containsKey("dbColumn") && map.containsKey("userColumn");
            MetadataDetail metadataDetail = new MetadataDetail();
            metadataDetail.setTableName(map.get("tableName"));
            metadataDetail.setWindColumn(map.get("windColumn"));
            metadataDetail.setDbColumn(map.get("dbColumn"));
            metadataDetail.setUserColumn(map.get("userColumn"));
            int ret = metadataDetailMapper.insert(metadataDetail);
            status = status && (ret == 1);
        }
        log.info("insertTableMetadataDetail: {} line", set.size());
        return status;
    }

    /**
     * @param userName 用户名
     * @param tableName 表名
     * @param description 对该表的描述
     * @return boolean
     */
    @Override
    public boolean insertTask(String userName, String tableName, String description){
        Task task = new Task();
        task.setUsername(userName);
        task.setTableName(tableName);
        task.setDescription(description);
        int ret = taskMapper.insert(task);
        log.info("insertTask: {} line", ret);
        return ret == 1;
    }

    /**
     * @param tableName 数据库表名
     * @return boolean
     */
    @Override
    public boolean deleteTableMetadata(String tableName) {
        QueryWrapper<Metadata> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("tableName", tableName);
        int ret = metadataMapper.delete(queryWrapper);
        log.info("deleteTableMetadata: {} line", ret);
        return ret == 1;
    }

    /**
     * @param tableName 数据库表名
     * @return boolean 删除的行数是否大于0，因为每个表至少有一个attribution
     */
    @Override
    public boolean deleteTableMetadataDetail(String tableName) {
        QueryWrapper<MetadataDetail> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("tableName", tableName);
        int ret = metadataDetailMapper.delete(queryWrapper);
        log.info("deleteTableMetadataDetail: {} line", ret);
        return ret >= 1;
    }

    /**
     * @param tableName 数据库表名
     * @return boolean
     */
    @Override
    public boolean deleteTask(String userName, String tableName) {
        return false;
    }
}
