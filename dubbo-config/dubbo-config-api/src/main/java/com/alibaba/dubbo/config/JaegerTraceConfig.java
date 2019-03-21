package com.alibaba.dubbo.config;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.jaeger.TraceUtils;
import io.jaegertracing.Configuration;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;

import java.io.Serializable;

/**
 * @author zhangjunfeng7
 * @program dubbo-parent
 * @description Jaeger调用链配置
 * @date 2019-02-22 15:12
 **/
public class JaegerTraceConfig extends AbstractConfig implements InitializingBean, BeanNameAware, Serializable {

    private static final long serialVersionUID = 8103478711466479530L;

    protected static final Logger logger = LoggerFactory.getLogger(JaegerTraceConfig.class);

    private String beanName;

    /** 是否开启调用链功能 */
    private boolean traceopen;

    /** 调用链地址 */
    private String address;

    /** 调用链采样类型 */
    private String type;

    /** 调用链采样率 */
    private double sampling;


    private transient static volatile boolean exported = false;

    public String getBeanName() {
        return beanName;
    }

    @Override
    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    public boolean isTraceopen() {
        return traceopen;
    }

    public void setTraceopen(boolean traceopen) {
        this.traceopen = traceopen;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public double getSampling() {
        return sampling;
    }

    public void setSampling(double sampling) {
        this.sampling = sampling;
    }

    @Override
    public void afterPropertiesSet(){
        if(exported){
            return;
        }
        exported = true;
        TraceUtils.setTraceOpen(this.traceopen);
        if(!traceopen){
            logger.info("Jaeger trace is not open!");
            return;
        }

        //默认值设置
        if(StringUtils.isEmpty(type)){
            type = "probabilistic";
        }
        if(sampling == 0) {
            switch (type) {
                case "const":
                    sampling = 1;
                    break;
                case "probabilistic":
                    sampling = 0.001;
                    break;
                case "ratelimiting":
                    sampling = 1;
            }
        }

        Configuration.SenderConfiguration senderConfiguration = new Configuration.SenderConfiguration();
        if(StringUtils.isEmpty(address)){
            address = System.getenv(Constants.CALLCHAIN_ADDRESS);
        }
        if(StringUtils.isEmpty(address)){
            logger.error("The callchain address is null");
            return;
        }
        senderConfiguration.withEndpoint("http://" + address + "/api/traces");
        TraceUtils.reporterConfiguration =new Configuration.ReporterConfiguration()
                .withSender(senderConfiguration)
                .withFlushInterval(1000)
                .withLogSpans(true);
        TraceUtils.samplerConfiguration = new Configuration.SamplerConfiguration()
                .withType(type)
                .withParam(sampling);
    }
}
