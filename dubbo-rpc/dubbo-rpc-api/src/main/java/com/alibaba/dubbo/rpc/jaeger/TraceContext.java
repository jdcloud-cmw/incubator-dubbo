package com.alibaba.dubbo.rpc.jaeger;

import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerTracer;

import java.io.Serializable;

/**
 * @program: jdsf-dubbo
 * @description: 调用链信息
 * @author: zhangjunfeng7
 * @create: 2018-11-08 17:01
 **/
public class TraceContext implements Serializable {

    private static final long serialVersionUID = 3135922578139860823L;

    private JaegerTracer tracer;

    private JaegerSpan span;

    public JaegerTracer getTracer() {
        return tracer;
    }

    public void setTracer(JaegerTracer tracer) {
        this.tracer = tracer;
    }

    public JaegerSpan getSpan() {
        return span;
    }

    public void setSpan(JaegerSpan span) {
        this.span = span;
    }
}
