/*
 *
 */
package org.redkalex.schedule.xxljob;

import org.redkale.schedule.ScheduleManager;
import org.redkale.schedule.spi.ScheduleManagerProvider;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public class XxljobScheduleProvider implements ScheduleManagerProvider {

    /**
     * &#60;xxljob addresses="http://localhost:8080/xxl-job-admin" executor="xxx" ip="127.0.0.1" port="5678" accessToken="default_token" /&#62;
     *
     * @param config 参数
     *
     * @return 是否识破
     */
    @Override
    public boolean acceptsConf(AnyValue config) {
        return config.getAnyValue("xxljob") != null;
    }

    @Override
    public ScheduleManager createInstance() {
        return new XxljobScheduleManager(null);
    }

}
