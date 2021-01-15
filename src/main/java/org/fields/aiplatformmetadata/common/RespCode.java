package org.fields.aiplatformmetadata.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum RespCode {
    SUCCESS(200, "操作成功"),
    FAILED(400, "操作失败"),
    UNAUTHORIZED(401, "未登录"),
    FORBIDDEN(403, "无权限"),
    SERVER_SCORE(12000,"服务积分不足");

    private long code;
    private String msg;

}
