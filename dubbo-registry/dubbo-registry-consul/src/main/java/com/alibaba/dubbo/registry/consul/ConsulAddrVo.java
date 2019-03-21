package com.alibaba.dubbo.registry.consul;

import java.util.Objects;

/**
 * @author zhangjunfeng7
 * @program dubbo-jdsf
 * @description consul地址信息
 * @date 2019-02-20 18:51
 **/
public class ConsulAddrVo {

    /** consul服务地址 */
    private String agentHost;

    /** consul服务端口 */
    private int agentPort;


    public String getAgentHost() {
        return agentHost;
    }

    public void setAgentHost(String agentHost) {
        this.agentHost = agentHost;
    }

    public int getAgentPort() {
        return agentPort;
    }

    public void setAgentPort(int agentPort) {
        this.agentPort = agentPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsulAddrVo that = (ConsulAddrVo) o;
        return agentPort == that.agentPort &&
                agentHost.equals(that.agentHost);
    }

    @Override
    public int hashCode() {

        return Objects.hash(agentHost, agentPort);
    }

    @Override
    public String toString() {
        return "ConsulAddrVo{" +
                "agentHost='" + agentHost + '\'' +
                ", agentPort=" + agentPort +
                '}';
    }
}
