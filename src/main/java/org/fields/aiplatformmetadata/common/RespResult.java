package org.fields.aiplatformmetadata.common;

import lombok.Data;

import java.io.Serializable;

@Data
public class RespResult<T> implements Serializable {
    private Long code;
    private String msg;
    private T data;

    public static <T> RespResult<T> success(T data) {
        RespResult<T> resp = new RespResult();
        resp.setCode(RespCode.SUCCESS.getCode());
        resp.setMsg(RespCode.SUCCESS.getMsg());
        resp.setData(data);
        return resp;
    }

    public static <T> RespResult<T> fail() {
        RespResult<T> resp = new RespResult();
        resp.setCode(RespCode.FAILED.getCode());
        resp.setMsg(RespCode.FAILED.getMsg());
        resp.setData(null);
        return resp;
    }

    public static <T> RespResult<T> fail(Long code) {
        RespResult<T> resp = new RespResult();
        resp.setCode(code);
        resp.setMsg(RespCode.FAILED.getMsg());
        resp.setData(null);
        return resp;
    }

    public static <T> RespResult<T> fail(Long code, String msg) {
        RespResult<T> resp = new RespResult();
        resp.setCode(code);
        resp.setMsg(msg);
        resp.setData(null);
        return resp;
    }

    public RespResult() {
        this.code = RespCode.SUCCESS.getCode();
        this.msg = RespCode.SUCCESS.getMsg();
    }

    public RespResult(Long code, String msg,T data){
        this.code = code;
        this.msg = msg;
        this.data = data;
    }
    public static Object badArgument() {
        return fail(500L, "参数不对");
    }

    public static Object badArgumentValue() {
        return fail(501L, "参数值不对");
    }


    public static Object serious() {
        return fail(502L, "系统内部错误");
    }

    public static Object unsupport() {
        return fail(503L, "业务不支持");
    }

    public static Object updatedDateExpired() {
        return fail(504L, "更新数据已经失效");
    }

    public static Object updatedDataFailed() {
        return fail(505L, "更新数据失败");
    }

    public static Object unauthz() {
        return fail(506L, "无操作权限");
    }
//    public static Object unGroup() {
//        return fail(507L, "拼团人数已满");
//    }
}
