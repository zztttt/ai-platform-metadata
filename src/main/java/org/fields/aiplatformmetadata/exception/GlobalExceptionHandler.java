package org.fields.aiplatformmetadata.exception;


import lombok.extern.slf4j.Slf4j;
import org.fields.aiplatformmetadata.common.ExceptionUtils;
import org.fields.aiplatformmetadata.common.RespCode;
import org.fields.aiplatformmetadata.common.RespResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.util.stream.Collectors;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(ApiException.class)
    public RespResult handler(ApiException e) {
        log.error(ExceptionUtils.getMessage(e));
        return RespResult.fail(RespCode.FAILED.getCode(),e.getMessage());
    }

    /**
     * 方法参数校验
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public RespResult handleMethodArgumentNotValidException(MethodArgumentNotValidException e) {
        log.error(ExceptionUtils.getMessage(e));
        return RespResult.fail(RespCode.FAILED.getCode(),e.getBindingResult().getFieldError().getDefaultMessage());
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public RespResult handleConstraintViolationException(ConstraintViolationException e){
        log.error(ExceptionUtils.getMessage(e));
        return RespResult.fail(RespCode.FAILED.getCode(),e.getConstraintViolations().stream().map(ConstraintViolation::getMessage).collect(Collectors.joining()));
    }

}
