package com.alibaba.dubbo.registry.consul;

import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.RpcException;
import com.ecwid.consul.SingleUrlParameters;
import com.ecwid.consul.UrlParameters;
import com.ecwid.consul.json.GsonFactory;
import com.ecwid.consul.transport.RawResponse;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;
import com.ecwid.consul.v1.QueryParams;
import com.google.gson.reflect.TypeToken;


import java.util.List;

/**
 * jsf封装consul客户端
 */
public class JdsfConsulClient extends ConsulClient {

    private ConsulRawClient rawClient;

    private JdsfConsulClient(ConsulRawClient rawClient){
        super(rawClient);
        this.rawClient = rawClient;
    }

    /**
     * 查找服务名实例列表
     * @param serviceName
     * @param onlyPassing
     * @param queryParams
     * @return
     */
    public List<JdsfHealthService> lookup(String serviceName, boolean onlyPassing, QueryParams queryParams, String dcName){

        UrlParameters tagParams =  new SingleUrlParameters("tag", "provider");
        UrlParameters passingParams = onlyPassing ? new SingleUrlParameters("status", "passing") : null;
        UrlParameters dcParams = StringUtils.isEmpty(dcName)? null:new SingleUrlParameters("dc", dcName);
        String url = "/v1/health/service/" + serviceName;
        RawResponse rawResponse = rawClient.makeGetRequest( url, tagParams, passingParams, dcParams, queryParams);

        if (rawResponse.getStatusCode() == 200) {
            List<JdsfHealthService> value = GsonFactory.getGson().fromJson(rawResponse.getContent(),
                    new TypeToken<List<JdsfHealthService>>() {
                    }.getType());
            return value;
        }
        throw new RpcException("Lookup serviceName:"+serviceName+" error!");
    }

    /**
     * 检查serviceId，是否实例已经注册
     * @param serviceId
     * @param serviceName
     * @param dcName
     * @return
     */
    public JdsfHealthService getServiceById(String serviceId, String serviceName, String dcName, String pid){
        UrlParameters dcParams = StringUtils.isEmpty(dcName)? null:new SingleUrlParameters("dc", dcName);
        UrlParameters passingParams = new SingleUrlParameters("status", "passing");
        UrlParameters tagParams = StringUtils.isEmpty(dcName)? null:new SingleUrlParameters("tag", pid);
        String url = "/v1/health/service/" + serviceName;
        RawResponse rawResponse = rawClient.makeGetRequest(url, tagParams, dcParams, passingParams, QueryParams.DEFAULT);
        if (rawResponse.getStatusCode() == 200) {
            List<JdsfHealthService> value = GsonFactory.getGson().fromJson(rawResponse.getContent(),
                    new TypeToken<List<JdsfHealthService>>(){}.getType());
            if(value == null || value.isEmpty()){
                return null;
            }
            for(JdsfHealthService service : value){
                if(service.getService().getId().equals(serviceId)){
                    return service;
                }
            }
        }
        return null;
    }

    public static JdsfConsulClient getInstance(String agentHost, int agentPort){
        ConsulRawClient rawClient = new ConsulRawClient(agentHost, agentPort);
        return new JdsfConsulClient(rawClient);
    }
}
