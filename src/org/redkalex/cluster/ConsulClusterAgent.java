/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cluster;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.redkale.boot.*;
import org.redkale.service.Service;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class ConsulClusterAgent extends ClusterAgent {

    protected String apiurl;

    protected Map<String, String> httpHeaders = Utility.ofMap("Content-Type", "application/json");

    @Override
    public void init(AnyValue config) {
        super.init(config);
        AnyValue[] properties = config.getAnyValues("property");
        for (AnyValue property : properties) {
            if ("apiurl".equalsIgnoreCase(property.getValue("name"))) {
                this.apiurl = property.getValue("value", "").trim();
                if (this.apiurl.endsWith("/")) this.apiurl = this.apiurl.substring(0, this.apiurl.length() - 1);
            }
        }
    }

    @Override
    public List<InetSocketAddress> queryAddress(NodeServer ns, String protocol, Service service) {
        String serviceid = generateServiceId(ns, protocol, service);
        String servicetype = generateServiceType(ns, protocol, service);
        InetSocketAddress address = ns.getSncpAddress();
        String json = "{\"ID\": \"" + serviceid + "\",\"Name\": \"" + servicetype + "\",\"Address\": \"" + address.getHostString() + "\",\"Port\": " + address.getPort() + "}";
        try {
            String rs = Utility.remoteHttpContent("PUT", this.apiurl + "/agent/service/register", httpHeaders, json).toString(StandardCharsets.UTF_8);
            System.out.println("注册:" + json + ", 结果：" + rs);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public void register(NodeServer ns, String protocol, Service service) {
        String serviceid = generateServiceId(ns, protocol, service);
        String servicetype = generateServiceType(ns, protocol, service);
        InetSocketAddress address = ns.getSncpAddress();
        String json = "{\"ID\": \"" + serviceid + "\",\"Name\": \"" + servicetype + "\",\"Address\": \"" + address.getHostString() + "\",\"Port\": " + address.getPort() + "}";
        try {
            String rs = Utility.remoteHttpContent("PUT", this.apiurl + "/agent/service/register", httpHeaders, json).toString(StandardCharsets.UTF_8);
            System.out.println("注册:" + json + ", 结果：" + rs);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void deregister(NodeServer ns, String protocol, Service service) {
        String serviceid = generateServiceId(ns, protocol, service);
        try {
            String rs = Utility.remoteHttpContent("PUT", this.apiurl + "/agent/service/deregister/" + serviceid, httpHeaders, (String) null).toString(StandardCharsets.UTF_8);
            System.out.println("注销:" + serviceid + ", 结果：" + rs);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
