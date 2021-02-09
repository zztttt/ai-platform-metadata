package org.fields.aiplatformmetadata;

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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
public class MybatisTest {
    @Autowired
    private MetadataMapper metadataMapper;
    @Autowired
    MetadataDetailMapper metadataDetailMapper;
    @Autowired
    TaskMapper taskMapper;

    public String dbColumn2windColumn(String tableName, String dbColumn) {
        log.info("dbColumn2windColumn. tableName: {}, dbColumn: {}", tableName, dbColumn);
        QueryWrapper<MetadataDetail> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("tableName", tableName).eq("dbColumn", dbColumn);
        MetadataDetail metadataDetail = metadataDetailMapper.selectOne(queryWrapper);
        if(metadataDetail == null){
            log.info("metadataDetail is null");
            throw new ApiException("metadataDetail is null");
        }
        return metadataDetail.getWindColumn();
    }
    @Test
    public void test(){
        String ret = dbColumn2windColumn("wind_CCommodityFuturesEODPrices_test", "s_info_windcode");
        log.info(ret);
    }

    @Test
    public void selectListTest() {
        List<Metadata> metadataList = metadataMapper.selectList(null);
        metadataList.forEach(System.out::println);
        List<MetadataDetail> metadataDetailList = metadataDetailMapper.selectList(null);
        metadataDetailList.forEach(System.out::println);
        List<Task> taskList = taskMapper.selectList(null);
        taskList.forEach(System.out::println);
    }

    @Test
    public void selectOneTest(){
        QueryWrapper<Metadata> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("tableName", "table1").eq("func", "wsd");
        Metadata metadata = metadataMapper.selectOne(queryWrapper);
        System.out.println(metadata);
    }

    @Test
    public void updateOneAndRestoreTest(){
        UpdateWrapper<Metadata> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("tableName", "table1").eq("func", "wsd")
                    .set("tableName", "new_table1").set("func", "WSD");
        int ret = metadataMapper.update(null, updateWrapper);
        Assert.assertEquals(1, ret);
        updateWrapper.clear();
        updateWrapper.eq("tableName", "new_table1").eq("func", "WSD")
                .set("tableName", "table1").set("func", "wsd");
        ret = metadataMapper.update(null, updateWrapper);
        Assert.assertEquals(1, ret);
    }

    @Test
    public void insertOneTest(){
        Metadata metadata = new Metadata();
        metadata.setTableName("?");
        metadata.setFunc("?");
        int ret = metadataMapper.insert(metadata);
        Assert.assertEquals(1, ret);
    }
}
