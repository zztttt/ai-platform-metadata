package org.fields.aiplatformmetadata.metadata.entity.request;

import lombok.Data;

import java.util.List;

@Data
public class CreateTable {
    private String oldTableName;
    private String newTableName;
    private String functionName;
    private String updateTime;
    private String updateUser;
    private List<String> windColumns;
    private List<String> dbColumns;
    private List<String> userColumns;
    private List<String> types;
}
