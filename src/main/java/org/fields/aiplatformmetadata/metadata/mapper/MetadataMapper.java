package org.fields.aiplatformmetadata.metadata.mapper;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.fields.aiplatformmetadata.metadata.entity.Metadata;
import org.springframework.stereotype.Component;

@Component
public interface MetadataMapper extends BaseMapper<Metadata> {

//    @Override
//    Metadata selectOne(Wrapper<Metadata> queryWrapper);
//
//    @Override
//    int insert(Metadata entity);
//
//    @Override
//    int update(Metadata entity, Wrapper<Metadata> updateWrapper);
}
