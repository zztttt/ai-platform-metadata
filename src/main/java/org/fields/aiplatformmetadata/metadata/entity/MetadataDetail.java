package org.fields.aiplatformmetadata.metadata.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("MetadataDetail")
public class MetadataDetail {
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;
    @TableField("tableName")
    private String tableName;
    @TableField("windColumn")
    private String windColumn;
    @TableField("dbColumn")
    private String dbColumn;
    @TableField("userColumn")
    private String userColumn;
}
