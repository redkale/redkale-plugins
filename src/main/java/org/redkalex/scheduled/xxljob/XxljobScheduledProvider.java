/*
 *
 */
package org.redkalex.scheduled.xxljob;

import org.redkale.util.AnyValue;
import org.redkale.scheduled.ScheduledManager;
import org.redkale.scheduled.spi.ScheduledManagerProvider;

/** @author zhangjx */
public class XxljobScheduledProvider implements ScheduledManagerProvider {

    /**
     * &#60;xxljob addresses="http://localhost:8080/xxl-job-admin" executorName="xxx" ip="127.0.0.1" port="5678"
     * accessToken="default_token" /&#62;
     *
     * @param config 参数
     * @return 是否适配xxljob
     */
    @Override
    public boolean acceptsConf(AnyValue config) {
        return config != null && config.getAnyValue("xxljob") != null;
    }

    @Override
    public ScheduledManager createInstance() {
        return new XxljobScheduledManager(null);
    }
}
