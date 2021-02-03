package org.fields.aiplatformmetadata.metadata.entity.request;

import lombok.Data;

import java.util.List;

@Data
public class UpdateTable {
    private String oldTableName;
    private String newTableName;
    private String updateTime;
    private String updateUser;
    private List<String> timeRange;
    private List<String> windCodes;
    private List<UpdateColumn> columns;
}
