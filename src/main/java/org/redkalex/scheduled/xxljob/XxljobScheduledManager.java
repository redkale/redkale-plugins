/*
 *
 */
package org.redkalex.scheduled.xxljob;

import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.logging.Level;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.Component;
import org.redkale.annotation.ResourceType;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.http.HttpServer;
import org.redkale.scheduled.Scheduled;
import org.redkale.scheduled.ScheduledEvent;
import org.redkale.scheduled.ScheduledManager;
import org.redkale.scheduled.spi.ScheduleManagerService;
import org.redkale.service.Local;
import org.redkale.service.RetResult;
import org.redkale.util.AnyValue;
import org.redkale.util.AnyValueWriter;
import org.redkale.util.RedkaleException;
import org.redkale.util.Utility;

/**
 * 配置项: &#60;xxljob addresses="http://localhost:8080/xxl-job-admin" executorName="xxx" ip="127.0.0.1" port="5678"
 * accessToken="default_token" /&#62;
 *
 * @author zhangjx
 */
@Local
@Component
@AutoLoad(false)
@ResourceType(ScheduledManager.class)
public class XxljobScheduledManager extends ScheduleManagerService {

    private final Map<String, XxljobTask> xxljobs = new ConcurrentHashMap<>();

    private final Map<Integer, XxljobTask> xxljobids = new ConcurrentHashMap<>();

    private XxljobConfig xxljobConfig;

    private HttpServer server;

    private RegistryParam registryParam;

    public XxljobScheduledManager(UnaryOperator<String> propertyFunc) {
        super(propertyFunc);
    }

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        if (!this.enabled) {
            return;
        }
        this.xxljobConfig = new XxljobConfig(conf.getAnyValue("xxljob"), this::getProperty);
        logger.log(Level.INFO, XxljobScheduledManager.class.getSimpleName() + " inited " + this.xxljobConfig);
    }

    @Override
    public void destroy(AnyValue conf) {
        super.destroy(conf);
        if (server != null) {
            try {
                if (registryParam != null) {
                    String regUrl = xxljobConfig.getDomain() + "/api/registryRemove";
                    String paramBody = JsonConvert.root().convertTo(registryParam);
                    String regResult = Utility.postHttpContent(regUrl, xxljobConfig.getHeaders(), paramBody);
                    logger.log(
                            Level.INFO,
                            XxljobScheduledManager.class.getSimpleName() + " registryRemove(" + regUrl + ") : "
                                    + regResult);
                }
                server.shutdown();
            } catch (Exception ex) {
                logger.log(Level.WARNING, XxljobScheduledManager.class.getSimpleName() + " shutdown error", ex);
            }
        }
    }

    /** 服务全部启动前被调用 */
    @Override
    public void onServersPreStart() {
        if (application.isCompileMode()) {
            return;
        }
        XxljobConfig clientConf = this.xxljobConfig;
        AnyValueWriter httpConf = AnyValueWriter.create()
                .addValue("name", "xxljob-httpserver")
                .addValue("host", clientConf.getIp())
                .addValue("port", clientConf.getPort());
        try {
            HttpServer http = new HttpServer(application);
            http.init(httpConf);
            addHttpServlet(http);
            this.server = http;
            http.start();
            // port可能配的是0，需要重新设置
            clientConf.setPort(http.getSocketAddress().getPort());
            // 注册
            RegistryParam regParam = new RegistryParam();
            regParam.setRegistryGroup("EXECUTOR");
            regParam.setRegistryKey(clientConf.getExecutorName());
            regParam.setRegistryValue("http://" + clientConf.getIp() + ":" + clientConf.getPort());
            String paramBody = JsonConvert.root().convertTo(regParam);
            String regUrl = clientConf.getDomain() + "/api/registry";
            String regResult = Utility.postHttpContent(regUrl, clientConf.getHeaders(), paramBody);
            this.registryParam = regParam;
            logger.log(
                    Level.INFO,
                    XxljobScheduledManager.class.getSimpleName() + " registry(" + regUrl + ")(" + paramBody + ") : "
                            + regResult);
        } catch (Exception ex) {
            throw new RedkaleException(
                    XxljobScheduledManager.class.getSimpleName() + " connect " + clientConf.getDomain()
                            + "/api/registry" + " error",
                    ex);
        }
    }

    private void addHttpServlet(HttpServer http) {
        final JsonConvert convert = JsonConvert.root();
        http.addHttpServlet("/beat", (request, response) -> {
                    response.finishJson(ReturnT.SUCCESS);
                })
                .addHttpServlet("/idleBeat", (request, response) -> {
                    IdleBeatParam param = convert.convertFrom(IdleBeatParam.class, request.getBody());
                    XxljobTask task = xxljobids.get(param.getJobId());
                    if (task == null || task.doing()) {
                        response.finishJson(
                                new ReturnT<String>(ReturnT.FAIL_CODE, "job thread is running or has trigger queue."));
                    } else {
                        response.finishJson(ReturnT.SUCCESS);
                    }
                })
                .addHttpServlet("/run", (request, response) -> {
                    TriggerParam param = convert.convertFrom(TriggerParam.class, request.getBody());
                    XxljobTask task = xxljobs.get(param.getExecutorHandler());
                    if (task == null) {
                        response.finishJson(new ReturnT<String>(
                                ReturnT.FAIL_CODE, "job handler [" + param.getExecutorHandler() + "] not found."));
                    } else {
                        if (task.jobid == 0) {
                            task.jobid = param.getJobId();
                            xxljobids.put(task.jobid, task);
                        }
                        ReturnT rs = null;
                        task.lock.lock();
                        try {
                            Map<String, Object> eventMap = task.eventMap();
                            if (eventMap != null) {
                                eventMap.clear();
                                eventMap.put("param", param.getExecutorParams());
                                eventMap.put("index", param.getBroadcastIndex());
                                eventMap.put("total", param.getBroadcastTotal());
                            }
                            rs = task.run();
                        } finally {
                            task.lock.unlock();
                        }
                        response.finishJson(rs);
                        HandleCallbackParam callbackParam = new HandleCallbackParam(
                                param.getLogId(), param.getLogDateTime(), rs.getCode(), rs.getMsg());
                        String callbackUrl = xxljobConfig.getDomain() + "/api/callback";
                        String callbackBody = JsonConvert.root().convertTo(new HandleCallbackParam[] {callbackParam});
                        Utility.postHttpContentAsync(callbackUrl, xxljobConfig.getHeaders(), callbackBody);
                    }
                })
                .addHttpServlet("/kill", (request, response) -> {
                    KillParam param = convert.convertFrom(KillParam.class, request.getBody());
                    XxljobTask task = xxljobids.get(param.getJobId());
                    if (task == null) {
                        response.finishJson(new ReturnT<String>(ReturnT.SUCCESS_CODE, "job thread already killed."));
                    } else {
                        task.stop();
                        response.finishJson(ReturnT.SUCCESS);
                    }
                })
                .addHttpServlet("/log", (request, response) -> {
                    response.finishJson(ReturnT.SUCCESS);
                });
    }

    @Override
    protected ScheduledTask createdOnlyNameTask(
            WeakReference ref,
            Method method,
            String name,
            String cron,
            String fixedDelay,
            String fixedRate,
            String initialDelay,
            String zone,
            TimeUnit timeUnit) {
        if (xxljobs.containsKey(name)) {
            throw new RedkaleException("@" + Scheduled.class.getSimpleName() + ".name (" + name + ") is repeat");
        }
        XxljobTask task = new XxljobTask(ref, name, method);
        xxljobs.put(name, task);
        return task;
    }

    protected class XxljobTask extends ScheduledTask {

        protected final ReentrantLock lock = new ReentrantLock();

        int jobid;

        private final Function<ScheduledEvent, Object> delegate;

        public XxljobTask(WeakReference ref, String name, Method method) {
            super(ref, name, method);
            this.delegate = createFuncJob(ref, method);
        }

        @Override
        protected Function<ScheduledEvent, Object> delegate() {
            return delegate;
        }

        public ReturnT run() {
            Object rs = super.execute();
            if (rs == null) {
                return ReturnT.SUCCESS;
            } else if (rs instanceof RetResult) {
                RetResult ret = (RetResult) rs;
                ReturnT rt = new ReturnT(ret.isSuccess() ? ReturnT.SUCCESS_CODE : ReturnT.FAIL_CODE, ret.getRetinfo());
                rt.setContent(ret.getResult());
                return rt;
            }
            return new ReturnT(rs);
        }

        @Override
        public void init() {
            // do nothing
        }

        @Override
        public void start() {
            // do nothing
        }
    }
}
