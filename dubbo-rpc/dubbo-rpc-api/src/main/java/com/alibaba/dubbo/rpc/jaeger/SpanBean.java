package com.alibaba.dubbo.rpc.jaeger;

import java.io.Serializable;
import java.util.Objects;

/**
 * @program: jdsf-dubbo
 * @description: 服务间调用链数据传送
 * @author: zhangjunfeng7
 * @create: 2018-11-13 18:06
 **/
public class SpanBean implements Serializable {

    private static final long serialVersionUID = 7368017061325593685L;
    private long traceIdHigh;
    private long traceIdLow;
    private long spanId;
    private long parentId;
    private byte flags;
    private String className;
    private String methodName;

    public long getTraceIdHigh() {
        return traceIdHigh;
    }

    public void setTraceIdHigh(long traceIdHigh) {
        this.traceIdHigh = traceIdHigh;
    }

    public long getTraceIdLow() {
        return traceIdLow;
    }

    public void setTraceIdLow(long traceIdLow) {
        this.traceIdLow = traceIdLow;
    }

    public long getSpanId() {
        return spanId;
    }

    public void setSpanId(long spanId) {
        this.spanId = spanId;
    }

    public long getParentId() {
        return parentId;
    }

    public void setParentId(long parentId) {
        this.parentId = parentId;
    }

    public byte getFlags() {
        return flags;
    }

    public void setFlags(byte flags) {
        this.flags = flags;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SpanBean spanBean = (SpanBean) o;
        return traceIdHigh == spanBean.traceIdHigh &&
                traceIdLow == spanBean.traceIdLow &&
                spanId == spanBean.spanId &&
                parentId == spanBean.parentId &&
                flags == spanBean.flags &&
                className.equals(spanBean.className) &&
                methodName.equals(spanBean.methodName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(traceIdHigh, traceIdLow, spanId, parentId, flags, className, methodName);
    }
}
