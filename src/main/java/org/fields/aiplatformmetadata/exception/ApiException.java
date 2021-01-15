package org.fields.aiplatformmetadata.exception;

public class ApiException extends RuntimeException{
    private String msg;

    public ApiException(String msg) {
        super(msg);
        this.msg = msg;
    }
}
