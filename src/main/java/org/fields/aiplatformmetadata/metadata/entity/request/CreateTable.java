package org.fields.aiplatformmetadata.metadata.entity.request;

import lombok.Data;

import java.util.List;

@Data
public class CreateTable {
    private String oldTableName;
    private String newTableName;
    private String updateTime;
    private String updateUser;
//    private String startStr;
//    private String endStr;
    private List<String> timeRange;
    private List<String> windCodes;
    private List<Column> columns;
    private String type;
//    private List<String> windColumns;
//    private List<String> dbColumns;
//    private List<String> userColumns;
}
