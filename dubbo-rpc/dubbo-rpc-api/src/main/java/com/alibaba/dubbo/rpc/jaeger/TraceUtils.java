package com.alibaba.dubbo.rpc.jaeger;


import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.ExecutionException;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.TimeoutException;
import com.alibaba.dubbo.remoting.exchange.ResponseFuture;
import com.alibaba.dubbo.rpc.*;
import com.alibaba.fastjson.JSON;
import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.JaegerTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Title: 分布式跟踪工具类<br>
 * <p/>
 * Description: 分布式跟踪埋点API封装<br>
 * <p/>
 * Company: <a href=www.jd.com>京东</a><br>
 *
 * @author zhangjunfeng7
 * <p/>
 * Created by juaby on 16-8-29.
 */
public class TraceUtils {

    private final static Logger LOGGER = LoggerFactory.getLogger(TraceUtils.class);

    private static final ThreadLocal<TraceContext> serverTraceLocal = new ThreadLocal<TraceContext>();
    private static final ConcurrentHashMap<String,TraceContext> spanCacheMap = new ConcurrentHashMap<>();

    private static volatile boolean isTraceOpen = false;

    private static String applicatonName;

    public static Configuration.ReporterConfiguration reporterConfiguration;

    public static Configuration.SamplerConfiguration samplerConfiguration;

    private static Object lock = new Object();

    /**
     * Server收到数据后
     *
     * @param invocation
     */
    public static void startTrace(Invocation invocation) {
        try {
            if (isTraceOpen() && isBizRequest(invocation)) {
                String spanbeanData = invocation.getAttachment(Constants.HIDDEN_KEY_TRACE_DATA);
                TraceContext traceContext = null;
                if(StringUtils.isEmpty(spanbeanData)){
                    return;
                } else {
                    SpanBean spanBean = JSON.parseObject(spanbeanData, SpanBean.class);
                    traceContext = startNewTracecontext(spanBean.getClassName(), spanBean.getMethodName(), RpcContext.getContext().getTraceContext());
                }
                if(traceContext != null) {
                    serverTraceLocal.set(traceContext);
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Trace-C-startRpc: data:{} error={}", invocation, e);
        }
    }

    /**
     * Server结束调用链
     * @param rpcResult
     */
    public static void endTrace(RpcResult rpcResult){
        endTraceWithThreadlocal(rpcResult, serverTraceLocal);
    }

    /**
     * for client 3-1) Client端发送数据包之前
     *
     * @param invocation
     */
    public static Invocation setContext(Invocation invocation) {
        try {
            boolean isBizRequest = isBizRequest(invocation);
            if (isTraceOpen() && isBizRequest){

                TraceContext traceContext = serverTraceLocal.get();
                if(traceContext == null) {
                    traceContext = startTraceAtSend(invocation);
                }
                JaegerSpanContext spanContext = traceContext.getSpan().context();
                SpanBean spanBean = new SpanBean();
                spanBean.setFlags(spanContext.getFlags());
                spanBean.setTraceIdHigh(spanContext.getTraceIdHigh());
                spanBean.setTraceIdLow(spanContext.getTraceIdLow());
                spanBean.setParentId(spanContext.getParentId());
                spanBean.setSpanId(spanContext.getSpanId());
                spanBean.setClassName(invocation.getInvoker().getInterface().getCanonicalName());
                spanBean.setMethodName(invocation.getMethodName());
                String spanBeanData = JSON.toJSONString(spanBean);
                invocation.getAttachments().put(Constants.HIDDEN_KEY_TRACE_DATA, spanBeanData);
                if(RpcContext.getContext().getTraceContext() == null) {
                    spanCacheMap.put(spanBeanData, traceContext);
                }
            }
        } catch (Throwable e) {
            if (invocation != null) {
                LOGGER.error("Trace-C-setContext: interfaceName={}; methodName={}; error={}",
                        invocation.getInvoker().getInterface().getCanonicalName(), invocation.getMethodName(), e);
            } else {
                LOGGER.error("Trace-C-setContext: error={}",e);
            }
        }
        return invocation;
    }

    /**
     * for client 3-2) Client端发送数据包之前
     *
     * @param invocation
     */
    public static ResponseFuture setTraceContext(Invocation invocation, ResponseFuture future) {
        try {
            if (isTraceOpen() && isBizRequest(invocation)) {
                if(future == null){
                    return null;
                }
                setContext(invocation);
                future.setSpanBean(invocation.getAttachment(Constants.HIDDEN_KEY_TRACE_DATA));
            }
        } catch (Throwable e) {
            if (invocation != null) {
                LOGGER.error("Trace-C-setTraceContext: interfaceName={}; methodName={}; error={}",
                        invocation.getInvoker().getInterface().getCanonicalName(), invocation.getMethodName(), e);
            } else {
                LOGGER.error("Trace-C-setTraceContext: error={}", e);
            }
        }
        return future;
    }


    /**
     * for client sync Client端收到服务端返回的数据之后
     *
     * @param response
     */
    public static void rpcClientRecv(Result response) {
        endTraceWithThreadlocal(response, null);
    }

    /**
     * for client failed
     *
     * @param cause
     */
    public static void rpcClientRecv(Invocation invocation, Throwable cause) {
        try {
            if (isTraceOpen()) {
                if(invocation.getAttachment(Constants.HIDDEN_KEY_TRACE_DATA) == null){
                    return;
                }
                String spanBeanData = invocation.getAttachment(Constants.HIDDEN_KEY_TRACE_DATA);
                TraceContext traceContext = spanCacheMap.remove(spanBeanData);
                if(RpcContext.getContext().getTraceContext() != null){
                    return;
                }
                String statusCode = getResultCode(cause);
                traceContext.getSpan().setTag("http.status_code", statusCode);
                if(cause != null) {
                    traceContext.getSpan().setTag("http.status_msg", cause.getClass().getName());
                }
                traceContext.getSpan().setTag("span.kind","client");
                traceContext.getSpan().finish();
                traceContext.getTracer().close();
            }
        } catch (Throwable throwable) {
            LOGGER.error("Trace-C-rpcClientRecv-1-sync: error={}", throwable);
        }
    }

    /**
     * 成功：200
     * 业务失败：500
     * Rpc异常：400
     * 超时：408
     * 其它错误：400
     * @param e
     * @return
     */
    private static String getResultCode(Throwable e) {
        if (e == null) {
            return "200";
        }
        if (e instanceof RpcException) {
            return "400";
        } else if(e instanceof RemotingException){
            return "502";
        }else if(e instanceof ExecutionException ) {
            return "500";
        } else if(e instanceof TimeoutException){
            return "408";
        } else {
            return "400";
        }
    }

    /**
     * 是否开启分布式跟踪
     *
     * @return
     */
    public static boolean isTraceOpen() {
        return isTraceOpen;
    }

    public static void setTraceOpen(boolean isTraceOpen){
        TraceUtils.isTraceOpen = isTraceOpen;
    }


    public static String getApplicatonName() {
        if(StringUtils.isEmpty(applicatonName)){
            synchronized (lock) {
                if(StringUtils.isEmpty(applicatonName)) {
                    applicatonName = RpcContext.getContext().getUrl().getParameter("application");
                    if(!StringUtils.isEmpty(applicatonName)){
                        applicatonName = applicatonName + "_";
                    }
                }
            }
            if(applicatonName == null){
                applicatonName = "";
            }
        }
        return applicatonName;
    }


    /**
     * 判断是否是业务接口请求
     *
     * @param invocation
     * @return
     */
    public static boolean isBizRequest(Invocation invocation) {
        return invocation instanceof RpcInvocation;
    }

    /**
     * Server收到数据后
     *
     * @param invocation
     */
    private static TraceContext startTraceAtSend(Invocation invocation) {
        TraceContext result = null;
        try {
            if (isTraceOpen() && isBizRequest(invocation)) {
//                String spanbeanData = invocation.getAttachment(Constants.HIDDEN_KEY_TRACE_DATA);
                result = startNewTracecontext(invocation.getInvoker().getInterface().getCanonicalName(), invocation.getMethodName(), null);
            }
        } catch (Throwable e) {
            LOGGER.error("Trace-C-startRpc: data:{} error={}", invocation, e);
        }
        return result;
    }

    /**
     * 调用链处理结束
     * @param result
     * @param threadLocal
     */
    private static void endTraceWithThreadlocal(Result result, ThreadLocal<TraceContext> threadLocal){
        try{
            if (isTraceOpen()) {
                //服务端逻辑检查
                if(threadLocal == null && RpcContext.getContext().getTraceContext() != null){
                    return;
                }
                TraceContext traceContext = threadLocal==null?null:threadLocal.get();
                //服务端调用链丢失，不做记录
                if(traceContext == null && (threadLocal != null || result.getAttachment(Constants.HIDDEN_KEY_TRACE_DATA) == null)) {
                    return;
                }
                if(result.getAttachment(Constants.HIDDEN_KEY_TRACE_DATA) != null){
                    String spanBeanData = result.getAttachment(Constants.HIDDEN_KEY_TRACE_DATA);
                    TraceContext cacheContext = spanCacheMap.remove(spanBeanData);
                    if(traceContext == null) {
                        traceContext = cacheContext;
                    }
                }

                if(traceContext == null){
                    return;
                }
                String statusCode = getResultCode(result.getException());
                traceContext.getSpan().setTag("http.status_code", statusCode);
                if(result.getException() != null) {
                    traceContext.getSpan().setTag("http.status_msg", result.getException().getClass().getName());
                }
                if(traceContext.getSpan().getReferences() != null && !traceContext.getSpan().getReferences().isEmpty()){
                    traceContext.getSpan().setTag("span.kind","server");
                } else {
                    traceContext.getSpan().setTag("span.kind","client");
                }
                traceContext.getSpan().finish();
                traceContext.getTracer().close();
            }
        } catch(Throwable e){
            LOGGER.error("Trace-C-rpcClientRecv-1-sync: error={}", e);
        } finally {
            if(threadLocal != null) {
                threadLocal.remove();
            }
        }
    }


    private static JaegerTracer getTracer(String className){
        Configuration configuration = new Configuration(getApplicatonName() + className);
        configuration.withSampler(samplerConfiguration);
        configuration.withReporter(reporterConfiguration);
        JaegerTracer tracer = configuration.getTracer();

        return tracer;
    }


    /**
     * 新建调用链上下文
     * @param className
     * @param methodName
     * @return
     */
    private static TraceContext startNewTracecontext(String className, String methodName, JaegerSpanContext spanContext){
        JaegerTracer tracer = getTracer(className);
        JaegerTracer.SpanBuilder spanBuilder = tracer.buildSpan(methodName);
        if(spanContext != null ){
            spanBuilder.asChildOf(spanContext);
        }
        JaegerSpan span = spanBuilder.start();
        TraceContext traceContext = new TraceContext();
        traceContext.setSpan(span);
        traceContext.setTracer(tracer);

        return traceContext;
    }

    /**
     * 构建Jaeger上下文
     * @param spanBean
     * @return
     */
    public static JaegerSpanContext convertToContext(SpanBean spanBean){
        JaegerSpanContext spanContext = new JaegerSpanContext(spanBean.getTraceIdHigh(), spanBean.getTraceIdLow(),
                spanBean.getSpanId(), spanBean.getParentId(), spanBean.getFlags());
        return spanContext;
    }
}