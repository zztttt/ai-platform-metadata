package org.fields.aiplatformmetadata.metadata.entity.request;

import lombok.Data;

@Data
public class UpdateColumn {
    private String windColumn;
    private String dbColumn;
    private String userColumn;
}
