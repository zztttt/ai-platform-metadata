package org.fields.aiplatformmetadata.common;


import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.TableField;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.Date;

@Data
public class BaseEntity {
    @TableField(fill = FieldFill.INSERT)
    @JsonIgnore
    private Date createTime;
    @TableField(fill = FieldFill.INSERT_UPDATE)
    @JsonIgnore
    private Date updateTime;
}
