package org.fields.aiplatformmetadata.metadata.service.Impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.exception.ApiException;
import org.fields.aiplatformmetadata.metadata.entity.Metadata;
import org.fields.aiplatformmetadata.metadata.entity.MetadataDetail;
import org.fields.aiplatformmetadata.metadata.entity.Task;
import org.fields.aiplatformmetadata.metadata.mapper.MetadataDetailMapper;
import org.fields.aiplatformmetadata.metadata.mapper.MetadataMapper;
import org.fields.aiplatformmetadata.metadata.mapper.TaskMapper;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

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
     * @param tableName 数据库表名
     * @return 该表是否已经存在
     */
    @Override
    public boolean isTableExisting(String tableName) {
        QueryWrapper<Metadata> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("tableName", tableName);
        Metadata metadata = metadataMapper.selectOne(queryWrapper);
        return metadata != null;
    }

    @Override
    public boolean isTaskExisting(String tableName) {
        // TODO
        return false;
    }

    /**
     * @param tableName 数据库表名
     * @return Metadata
     */
    @Override
    public Metadata queryMetadata(String tableName) {
        QueryWrapper<Metadata> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("tableName", tableName);
        Metadata metadata = metadataMapper.selectOne(queryWrapper);
        return metadata;
    }

    /**
     * @param tableName 数据库表名
     * @param windColumn 指定的wind field
     * @return boolean
     */
    @Override
    public boolean isColumnExist(String tableName, String windColumn) {
        QueryWrapper<MetadataDetail> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("tableName", tableName).eq("windColumn", windColumn);
        MetadataDetail metadataDetail = metadataDetailMapper.selectOne(queryWrapper);
        return metadataDetail == null? false: true;
    }

    private MetadataDetail queryMetadataDetailFromUserColumn(String tableName, String userColumn){
        QueryWrapper<MetadataDetail> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("tableName", tableName).eq("userColumn", userColumn);
        MetadataDetail metadataDetail = metadataDetailMapper.selectOne(queryWrapper);
        if(metadataDetail == null){
            log.info("queryMetadataDetailFromUserColumn error");
            throw new ApiException("queryMetadataDetailFromUserColumn error");
        }
        return metadataDetail;
    }
    @Override
    public String queryDbColumnFromUserColumn(String tableName, String userColumn) {

        return queryMetadataDetailFromUserColumn(tableName, userColumn).getDbColumn();
    }

    @Override
    public String queryWindColumnFromUserColumn(String tableName, String userColumn) {
        return queryMetadataDetailFromUserColumn(tableName, userColumn).getWindColumn();
    }

    @Override
    public String queryTypeFromUserColumn(String tableName, String userColumn) {
        return queryMetadataDetailFromUserColumn(tableName, userColumn).getType();
    }

    /**
     * @param tableName 表名
     * @param func 该表所用的函数
     * @return boolean
     */
    @Override
    public boolean insertTableMetadata(String tableName, String func, String updateTime, String updateUser){
        if(isTableExisting(tableName)){
            log.info("insertTableMetadata error: table {} is already existing", tableName);
            throw new ApiException("insertTableMetadata error: table is already existing");
        }
        Metadata metadata = new Metadata();
        metadata.setTableName(tableName);
        metadata.setFunc(func);
        metadata.setUpdateTime(updateTime);
        metadata.setUpdateUser(updateUser);
        int ret = metadataMapper.insert(metadata);
        log.info("insertTableMetadata: {} line", ret);
        return ret == 1;
    }
    public boolean insertTableMetadataOneDetail(String tableName, String windColumn, String dbcolumn, String userColumn, String type){
        MetadataDetail metadataDetail = new MetadataDetail();
        metadataDetail.setTableName(tableName);
        metadataDetail.setWindColumn(windColumn);
        metadataDetail.setDbColumn(dbcolumn);
        metadataDetail.setUserColumn(userColumn);
        metadataDetail.setType(type);
        // query whether already exists
        QueryWrapper<MetadataDetail> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("tableName", tableName)
                    .eq("windColumn", windColumn);
        if(metadataDetailMapper.selectOne(queryWrapper) != null){
            log.info("insertTableMetadataOneDetail error. {} {} is already exists", tableName, windColumn);
            throw new ApiException("insertTableMetadataOneDetail error");
        }
        int ret = metadataDetailMapper.insert(metadataDetail);
        return ret == 1;
    }

    /**
     * @param list 包含对一个table的每个字段的具体值的说明。 eg tableName，windColumn， dbColumn, userColumn, type
     *        map 对MetadataDetail的的值进行说明
     * @return boolean
     */
    @Override
    public boolean insertTableMetadataDetail(List<Map<String, String>> list){
        boolean status = true;
        for(Map<String, String> map: list){
            assert map.size() == 5 && map.containsKey("tableName") && map.containsKey("windColumn")
                    && map.containsKey("dbColumn") && map.containsKey("userColumn") && map.containsKey("type");
            boolean ret = insertTableMetadataOneDetail(map.get("tableName"), map.get("windColumn"), map.get("dbColumn"), map.get("userColumn"), map.get("type"));
            status = status && ret;
        }
        log.info("insertTableMetadataDetail: {} line", list.size());
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
        if(!isTableExisting(tableName)){
            log.info("deleteTableMetadata error: table {} is not existing", tableName);
            throw new ApiException("deleteTableMetadata error: table is not existing");
        }
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
        // TODO
        return false;
    }

    /**
     * @param tableName 数据库表名
     * @param updateTime 最后一次更新时间
     * @param updateUser 最后一次更新用户
     * @return boolean
     */
    @Override
    public boolean updateMetadata(String tableName, String updateTime, String updateUser) {
        UpdateWrapper<Metadata> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("tableName", tableName).set("updateTime", updateTime).set("updateUser", updateUser);
        int ret = metadataMapper.update(null, updateWrapper);
        return ret == 1;
    }
}
