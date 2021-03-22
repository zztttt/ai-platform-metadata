package org.fields.aiplatformmetadata.metadata.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

@Data
public class Task {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    @TableField("userName")
    private String username;
    @TableField("tableName")
    private String tableName;
    @TableField("description")
    private String description;
    @TableField("parameter")
    private String parameter;
    @TableField("status")
    private String status;
}
