package org.fields.aiplatformmetadata.metadata.entity;

import lombok.Data;

import java.util.List;

@Data
public class RequestDemo {
    private long id;
    private String name;
    private List<String> list;
}
