package com.alibaba.dubbo.registry.consul;


import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.registry.NotifyListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.mock;

public class ConsulRegistryTest {

    private String service = "org.apache.dubbo.test.injvmServie";
    private URL serviceUrl = URL.valueOf("consul://consul/" + service + "?notify=false&methods=test1,test2");
    private ConsulRegistry consulRegistry;
    private URL registryUrl;

    @Before
    public void setUp() throws Exception {
        int consulPort = 8500;
        this.registryUrl = URL.valueOf("consul://10.12.209.43:8500");

        consulRegistry = (ConsulRegistry) new ConsulRegistryFactory().createRegistry(registryUrl);
    }

    @After
    public void testDown(){
        consulRegistry.destroy();
    }

    @Test
    public void testDoRegister(){
        consulRegistry.doRegister(serviceUrl);
        System.out.println();
    }

    @Test
    public void testLookUp(){
        URL providerUrl = URL.valueOf("dubbo://localhost:9000/" + service + "?notify=false&methods=test1,test2");
        consulRegistry.doRegister(providerUrl);
        List<URL> resultList = consulRegistry.lookup(serviceUrl);
        System.out.println();
    }

    @Test
    public void testDoUnregister(){
        consulRegistry.doRegister(serviceUrl);
        consulRegistry.doUnregister(serviceUrl);
        System.out.println();
    }

    @Test
    public void testDoSubscribe(){
        NotifyListener listener = mock(NotifyListener.class);
        consulRegistry.doSubscribe(serviceUrl, listener);
        URL providerUrl = URL.valueOf("dubbo://localhost:9000/" + service + "?notify=false&methods=test1,test2");
        consulRegistry.doRegister(providerUrl);
        while (true){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}