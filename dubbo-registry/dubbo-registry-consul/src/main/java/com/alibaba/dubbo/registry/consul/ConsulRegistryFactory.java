package com.alibaba.dubbo.registry.consul;


import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.support.AbstractRegistryFactory;

/**
 * @author zhangjunfeng7
 * @program dubbo-jdsf
 * @description consul注册中心工厂类
 * @date 2019-02-20 17:21
 **/
public class ConsulRegistryFactory extends AbstractRegistryFactory {

    @Override
    protected Registry createRegistry(URL url) {
        return new ConsulRegistry(url);
    }
}
