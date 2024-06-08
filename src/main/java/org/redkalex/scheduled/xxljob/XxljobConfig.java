package org.redkalex.scheduled.xxljob;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;
import org.redkale.convert.ConvertColumn;
import org.redkale.convert.json.JsonConvert;
import org.redkale.util.AnyValue;
import org.redkale.util.Utility;

/** @author zhangjx */
public class XxljobConfig {

    // 不以/结尾
    @ConvertColumn(ignore = true)
    private String domain;

    private String addresses;

    private String ip;

    private int port;

    private String executorName;

    @ConvertColumn(ignore = true)
    private String accessToken;

    @ConvertColumn(ignore = true)
    private Map<String, Serializable> headers;

    public XxljobConfig() {}

    public XxljobConfig(AnyValue conf, UnaryOperator<String> func) {
        this.addresses =
                Objects.requireNonNull(func.apply(conf.getValue("addresses").trim()));
        this.executorName = Objects.requireNonNull(func.apply(conf.getValue("executorName")));
        this.ip = func.apply(conf.getValue("ip", Utility.localInetAddress().getHostAddress()));
        this.port = conf.getIntValue("port", 0);
        this.accessToken = func.apply(conf.getValue("accessToken", ""));
        this.headers = new HashMap<>();
        this.headers.put("XXL-JOB-ACCESS-TOKEN", this.accessToken);
        if (this.addresses.endsWith("/")) {
            this.domain = this.addresses.substring(0, this.addresses.length() - 1);
        } else {
            this.domain = this.addresses;
        }
    }

    public Map<String, Serializable> getHeaders() {
        return headers;
    }

    public String getDomain() {
        return domain;
    }

    public String getAddresses() {
        return addresses;
    }

    public void setAddresses(String addresses) {
        this.addresses = addresses;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getExecutorName() {
        return executorName;
    }

    public void setExecutorName(String executorName) {
        this.executorName = executorName;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
