package com.alibaba.dubbo.registry.consul;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.rpc.RpcException;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.Member;
import com.ecwid.consul.v1.agent.model.NewService;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangjunfeng7
 * @program dubbo-jdsf
 * @description consul注册中心
 * @date 2019-02-20 17:20
 **/
public class ConsulRegistry extends FailbackRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ConsulRegistry.class);

    /** consul注册中心默认端口 */
    private final static int DEFAULT_ZOOKEEPER_PORT = 8500;

    /** 默认根路径——group */
    private final static String DEFAULT_ROOT = "dubbo";

    private final static String REP_COMMA = "__";

    /** 服务名分隔符 */
    private final static String SERVICE_COMMA = ":::";

    private final static String TTL_CHECK_PREFIX  = "service:";

    /**  */
    private final String root;

    /** 当前使用的consul节点 */
    private JdsfConsulClient currentConsulClient;

    /** 是否可用 */
    private volatile boolean available = false;

    /** 当前连接consul节点地址信息 */
    private ConsulAddrVo currentConsulAddr;

    /**
     * 配置的注册地址列表
     */
    private Vector<ConsulAddrVo> registryAddressList;

    /** 集群中所有 */
    private Vector<JdsfConsulClient> jsfConsulClientList = new Vector<>();

    /** 失败的注册的urls */
    private final ConcurrentHashSet<URL> failed_register_urls = new ConcurrentHashSet<URL>();

    /** 已注册的urls */
    private final ConcurrentHashSet<URL> registered_urls = new ConcurrentHashSet<URL>();

    /** 订阅监听列表 */
    private final ConcurrentHashMap<URL,Set<NotifyListener>> subscribeMap = new ConcurrentHashMap<>();

    private final ConcurrentHashSet<String> ttlCheckSet = new ConcurrentHashSet<String>();

    /** 心跳定时器线程 + 定时重试失败服务 */
    private ScheduledService retryScheduledService;

    /** 定时查询线程 */
    private ScheduledService checkerScheduledService;

    /** 定时consumer存活线程 */
    private ScheduledService ttlCheckScheduledService;

    public ConsulRegistry(URL url){
        super(url);
        if (url.isAnyHost() && StringUtils.isEmpty(System.getenv(Constants.REGISTRY_ADDRESS))) {
            throw new IllegalStateException("registry address == null");
        }
        String group = url.getParameter(Constants.GROUP_KEY, DEFAULT_ROOT);

        this.root = group;
        String host = url.getHost();
        if(StringUtils.isEmpty(host)){
            host = System.getenv(Constants.REGISTRY_ADDRESS);
            registryAddressList = this.parseRegistryAddress(host);
        } else {
            int port = url.getPort();
            if (port == 0) {
                port = DEFAULT_ZOOKEEPER_PORT;
            }
            registryAddressList = this.parseRegistryAddress(host+":" + port);
        }
        this.initConsulList();
        startDaemonThread();
    }

    @Override
    public void doRegister(URL url) {
        this.checkConnection();
        String insKey = toServiceKey(url);
        String serviceName = toServiceName(url);
        NewService service = new NewService();
        service.setId(insKey);
        service.setMeta(replaceComma(url.getParameters()));
        service.setAddress(url.getIp());
        service.setName(serviceName);
        service.setPort(url.getPort());

        List<String> tagsList = new ArrayList<String>();
        tagsList.add("protocol=" + url.getProtocol());
        if(StringUtils.isNotEmpty(url.getUsername())) {
            tagsList.add("username=" + url.getUsername());
        }
        if(StringUtils.isNotEmpty(url.getPassword())){
            tagsList.add("password=" + url.getPassword());
        }
        tagsList.add("host=" + url.getHost());
        if(StringUtils.isNotEmpty(url.getPath())){
            tagsList.add("path=" + url.getPath());
        }
        if(url.getPort() > 0){
            tagsList.add("provider");
        } else {
            tagsList.add("consumer");
        }
        service.setTags(tagsList);
        //存活检查
        if(url.getPort() != 0) {
            NewService.Check check = new NewService.Check();
            check.setTcp(url.getIp() + ":" + url.getPort());
            check.setInterval("10s");
            check.setTimeout("1s");
            check.setStatus("passing");
            service.setCheck(check);
        } else {
            NewService.Check check = new NewService.Check();
            check.setTtl("30s");
            check.setStatus("passing");
            service.setCheck(check);
            ttlCheckSet.add(TTL_CHECK_PREFIX + insKey);
        }

        boolean successed = false;
        try{
            currentConsulClient.agentServiceRegister(service);
            successed = true;
        } catch(Exception e){
            logger.error("client:{" + currentConsulAddr.getAgentHost() + "} registry failed!", e);
        }
        if(successed){
            registered_urls.add(url);
        } else {
            failed_register_urls.add(url);
        }
    }

    @Override
    public void doUnregister(URL url) {
        checkConnection();
        String insKey = toServiceKey(url);
        String serviceName = toServiceName(url);
        for (JdsfConsulClient consulClient : this.jsfConsulClientList) {
            try {
                String pid = url.getParameter(Constants.PID_KEY, "");
                JdsfHealthService service = consulClient.getServiceById(insKey, serviceName, null, pid);
                if (service != null ) {
                    consulClient.agentServiceDeregister(insKey);
                    break;
                }
            } catch (Exception e) {
                logger.error("doUnRegister failed: id:{" + insKey + "}", e);
            }
        }
    }

    @Override
    public void doSubscribe(URL url, NotifyListener listener) {
        if(Constants.PROVIDER_PROTOCOL.equals(url.getProtocol())){
            return;
        }
        Set<NotifyListener> listenerSet = subscribeMap.get(url);
        if(listenerSet == null){
            listenerSet = new ConcurrentHashSet<>();
            subscribeMap.put(url, listenerSet);
        }
        listenerSet.add(listener);
        doSubscribeCheck();
    }

    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
        Set<NotifyListener> listenerSet = subscribeMap.get(url);
        if(listenerSet == null){
            return;
        }
        listenerSet.remove(listener);
    }

    @Override
    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 初始化consul地址列表
     */
    synchronized private void initConsulList(){
        List<JdsfConsulClient> consulClientList = new ArrayList<>();
        checkConnection();
        try{
            Set<ConsulAddrVo> consulAddrVoSet = new HashSet<>();
            Response<List<Member>> response = currentConsulClient.getAgentMembers();
            for( Member member : response.getValue()){
                if(member.getStatus() != 1){
                    continue;
                }
                consulClientList.add(JdsfConsulClient.getInstance(member.getName(), currentConsulAddr.getAgentPort()));
                ConsulAddrVo consulAddrVo = new ConsulAddrVo();
                consulAddrVo.setAgentHost(member.getName());
                consulAddrVo.setAgentPort(this.currentConsulAddr.getAgentPort());
                consulAddrVoSet.add(consulAddrVo);
            }
            //
            if(this.registryAddressList.size() != consulAddrVoSet.size()){
                synchronized (this.registryAddressList){
                    this.registryAddressList.clear();
                    this.registryAddressList.addAll(consulAddrVoSet);
                }
            }
        } catch(Exception e){
            logger.warn("initConsulList failed:", e);
        }
        this.jsfConsulClientList.clear();
        this.jsfConsulClientList.addAll(consulClientList);
        return;
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            if (retryScheduledService != null) {
                retryScheduledService.shutdown();
                retryScheduledService = null;
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        try {
            if (checkerScheduledService != null) {
                checkerScheduledService.shutdown();
                checkerScheduledService = null;
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }


    /**
     * 分析地址，并初始化地址列表
     * @param address
     * @return
     */
    private Vector<ConsulAddrVo> parseRegistryAddress(String address){
        List<String> addressList = Arrays.asList(address.split(","));
        Set<ConsulAddrVo> consulAddrVoSet = new HashSet<>();
        for(String addr : addressList){
            try{
                String hostPort[] = addr.split(":");
                String host = hostPort[0];
                Integer port = 8500;
                if(hostPort.length == 2){
                    port = Integer.parseInt(hostPort[1]);
                }
                Response<List<Member>> response = JdsfConsulClient.getInstance(host, port).getAgentMembers();
                for( Member member : response.getValue()){
                    if(member.getStatus() != 1){
                        continue;
                    }
                    ConsulAddrVo consulAddrVo = new ConsulAddrVo();
                    consulAddrVo.setAgentHost(member.getName());
                    consulAddrVo.setAgentPort(port);
                    consulAddrVoSet.add(consulAddrVo);
                }
            } catch(Exception e){
                logger.error("Initing register cluster exeception happend.", e);
            }
        }
        return new Vector<>(consulAddrVoSet);
    }

    /**
     * 检查consul连接是否可用
     */
    private void checkConnection(){
        if(currentConsulClient == null){
            this.currentConsulAddr = null;
            this.currentConsulClient = null;
            this.reconnect();
        }
        try {
            currentConsulClient.getAgentMembers();
        } catch (Exception e){
            logger.error("current consul:{"+this.currentConsulAddr+"} cannot connected!", e);
            this.currentConsulAddr = null;
            this.currentConsulClient = null;
            this.reconnect();
        }
    }

    /**
     * 重新获取可用Consul客户端
     */
    public void reconnect(){
        if(this.registryAddressList.isEmpty()){
            this.available = false;
            return;
        }
        boolean retryInit = false;
        if(this.registryAddressList.size() == 1){
            ConsulAddrVo vo = this.registryAddressList.get(0);
            JdsfConsulClient consulClient = JdsfConsulClient.getInstance(vo.getAgentHost(), vo.getAgentPort());
            try {
                Response<List<Member>> memberResponse = consulClient.getAgentMembers();
                if (memberResponse.getValue() != null && memberResponse.getValue().size() > 0) {
                    if (currentConsulClient == null) {
                        this.currentConsulAddr = vo;
                        this.currentConsulClient = consulClient;
                    }
                    if(memberResponse.getValue().size() != this.registryAddressList.size()){
                        retryInit = true;
                    }
                }
            } catch (Exception e) {
                logger.error("Address:{"+vo+"} is not available!", e);
            }
        } else {
            Random random = new Random();
            int index = random.nextInt(this.registryAddressList.size());
            int counter = 0;
            for (int i = index; counter < this.registryAddressList.size(); i = (++i) % this.registryAddressList.size(), counter++) {
                ConsulAddrVo vo = this.registryAddressList.get(i);
                JdsfConsulClient consulClient = JdsfConsulClient.getInstance(vo.getAgentHost(), vo.getAgentPort());
                try {
                    Response<List<Member>> memberResponse = consulClient.getAgentMembers();
                    if (memberResponse.getValue() != null && memberResponse.getValue().size() > 0) {
                        if(memberResponse.getValue().size() != this.registryAddressList.size()){
                            retryInit = true;
                        }
                        if (currentConsulClient == null) {
                            this.currentConsulAddr = vo;
                            this.currentConsulClient = consulClient;
                            break;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Address:{"+vo+"} is not available!", e);
                }
            }
        }
        if(retryInit){
            this.initConsulList();
        }
        if(this.currentConsulClient == null){
            this.available = false;
        } else {
            this.available = true;
        }
    }

    /**
     * 获取服务Key
     * @param url
     * @return
     */
    private String toServiceKey(URL url) {
        String key = toServiceName(url);

        String host = url.getHost();
        if(StringUtils.isNotEmpty(host)){
            key += "_" + host;
        }
        key += ":" +url.getPort();
        String pid = url.getParameter(Constants.PID_KEY, "");
        if(StringUtils.isNotEmpty(pid)){
            key += "_" + pid;
        }
        return key;
    }

    private String toServiceName(URL url) {
        String version = url.getParameter(Constants.VERSION_KEY, "");
        if(StringUtils.isNotEmpty(version)){
            version = "_" + version;
        }
        return url.getServiceInterface() + SERVICE_COMMA + root +version;
    }

    public boolean doCheckRegister(URL url) throws RpcException {
        checkConnection();
        String insKey = toServiceKey(url);
        JdsfHealthService healthService = currentConsulClient.getServiceById(insKey, url.getServiceInterface(), null, null);
        if (healthService != null) {
            return true;
        }
        return false;
    }

    /**
     * 查找服务可用服务端列表
     * @param url
     * @return
     * @throws RpcException
     */
    @Override
    public List<URL> lookup(URL url) throws RpcException {
        checkConnection();
        List<JdsfHealthService> allServiceList = null;
        try {
            allServiceList = currentConsulClient.lookup(toServiceName(url), true, QueryParams.DEFAULT, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (allServiceList == null) {
            return Collections.EMPTY_LIST;
        }
        List<URL> resultList = new ArrayList<>(allServiceList.size());
        for (JdsfHealthService healthService : allServiceList){
            JdsfHealthService.Service service = healthService.getService();
            String protocol = null;
            String username = null;
            String password = null;
            String host = null;
            String path = null;
            Map<String, String> parameters = recoverComma(service.getMeta());

            for(String tag : service.getTags()) {
                String values[] = tag.split("=");
                if (values.length < 2) {
                    continue;
                }
                switch (values[0]){
                    case "protocol":
                        protocol = tag.substring("protocol=".length());
                        break;
                    case "username":
                        username = tag.substring("username=".length());
                        break;
                    case "password":
                        password = tag.substring("password=".length());
                        break;
                    case "host":
                        host = tag.substring("host=".length());
                        break;
                    case "path":
                        path = tag.substring("path=".length());
                        break;
                }
            }
            URL serviceUrl = new URL(protocol,username, password, host, service.getPort(), path, parameters);
            resultList.add(serviceUrl);
        }
        return resultList;
    }


    /**
     * 重试注册的间隔
     */
    private final int retryPeriod = 30000;

    /** 订阅检查间隔 */
    private final int checkPeriod = 30000;

    /** 订阅检查间隔 */
    private final int ttlPeriod = 10000;

    /**
     * Start daemon thread.
     */
    public void startDaemonThread() {

        // 注册检查定时器
        if (retryScheduledService == null) {
            retryScheduledService = new ScheduledService("dubbo-consulRegistry-retry",
                    ScheduledService.MODE_FIXEDDELAY,
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                for(URL jsfUrl : failed_register_urls){
                                    if(!doCheckRegister(jsfUrl)){
                                        doRegister(jsfUrl);
                                    }
                                }
                                for(URL jsfUrl : registered_urls){
                                    if(!doCheckRegister(jsfUrl)) {
                                        doRegister(jsfUrl);
                                    }
                                }
                            } catch (Throwable e) {
                                logger.error("Error when send heartbeat and retry", e);
                            }
                        }
                    }, //定时load任务
                    retryPeriod, // 延迟一个周期
                    retryPeriod, // 一个周期循环
                    TimeUnit.MILLISECONDS
            ).start();
        }
        //订阅
        if (checkerScheduledService == null) {
            checkerScheduledService = new ScheduledService("dubbo-consulSubscribe-check",
                    ScheduledService.MODE_FIXEDDELAY,
                    new Runnable() {
                        @Override
                        public void run() {
                            doSubscribeCheck();
                        }
                    }, //定时load任务
                    checkPeriod, // 延迟一个周期
                    checkPeriod, // 一个周期循环
                    TimeUnit.MILLISECONDS
            ).start();
        }
        if(ttlCheckScheduledService == null){
            ttlCheckScheduledService = new ScheduledService("dubbo-servicettl-check", ScheduledService.MODE_FIXEDDELAY,
                    new Runnable() {
                        @Override
                        public void run() {
                            checkConnection();
                            for (String checkId : ttlCheckSet){
                                try{
                                    currentConsulClient.agentCheckPass(checkId);
                                } catch(Exception e){
                                    logger.error("TTL check failed: checkId:{"+checkId+"}, msg:", e);
                                }
                            }
                        }
                    },
                    ttlPeriod,
                    ttlPeriod,
                    TimeUnit.MILLISECONDS).start();
        }
    }

    /**
     * 执行订阅
     *
     * @return
     */
    private boolean doSubscribeCheck(){
        try {
            for (Map.Entry<URL, Set<NotifyListener>> entry : subscribeMap.entrySet()) {
                URL url = entry.getKey();
                Set<NotifyListener> listenerSet = entry.getValue();
                if(listenerSet == null || listenerSet.isEmpty()){
                    continue;
                }
                List<URL> providerList = lookup(url);
                //如果返回的服务端列表为空
                if (providerList == null || providerList.isEmpty()) {
                    continue;
                }

                for(NotifyListener listener : entry.getValue()) {
                    doNotify(url, listener,providerList);
                }
            }
        } catch (Throwable e) {
            logger.error("Error when subscribing", e);
        }
        return true;
    }

    /**
     * 替换掉参数中的特殊字符 .
     * @param parameters
     * @return
     */
    private Map<String,String> replaceComma(Map<String, String> parameters){
        if(parameters == null || parameters.isEmpty()){
            return parameters;
        }
        Map<String,String> resultMap = new HashMap<>(parameters);
        for(String key : parameters.keySet()){
            String oldKey = key;
            if (key.indexOf(".") != -1){
                key = key.replaceAll("\\.", REP_COMMA);
                String value = parameters.get(oldKey);
                resultMap.remove(oldKey);
                resultMap.put(key,value);
            }
        }
        return resultMap;
    }


    private Map<String,String> recoverComma(Map<String, String> parameters){
        if(parameters == null || parameters.isEmpty()){
            return parameters;
        }
        Map<String,String> resultMap = new HashMap<>(parameters);
        for(String key : parameters.keySet()){
            String oldKey = key;
            if (key.indexOf(REP_COMMA) != -1){
                key = key.replaceAll(REP_COMMA, ".");
                String value = parameters.get(oldKey);
                resultMap.remove(oldKey);
                resultMap.put(key,value);
            }
        }
        return resultMap;
    }

}
