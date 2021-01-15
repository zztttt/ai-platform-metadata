package org.fields.aiplatformmetadata.metadata.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("Metadata")
public class Metadata {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    @TableField("tableName")
    private String tableName;
    @TableField("func")
    private String func;
    @TableField("updateTime")
    private String updateTime;
    @TableField("updateUser")
    private String updateUser;
}
