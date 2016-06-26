/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.*;
import static org.redkalex.pay.Pays.PAYWAY_WEB;

/**
 *
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
public class PayCreatRequest extends PayRequest {

    protected short payway = PAYWAY_WEB;

    protected long trademoney; //  支付金额。 单位:分 

    protected String tradetitle = ""; //订单标题

    protected String tradebody = ""; //订单内容描述

    protected int paytimeout = 300; //支付超时的秒数

    protected String clientAddr = "";  //客户端IP地址

    protected Map<String, String> map; //扩展信息

    @Override
    public void checkVaild() {
        super.checkVaild();
        if (this.trademoney < 1) throw new RuntimeException("trademoney is illegal");
        if (this.tradetitle == null || this.tradetitle.isEmpty()) throw new RuntimeException("tradetitle is illegal");
        if (this.tradebody == null || this.tradebody.isEmpty()) throw new RuntimeException("tradebody is illegal");
        if (this.clientAddr == null || this.clientAddr.isEmpty()) throw new RuntimeException("clientAddr is illegal");
        if (this.paytimeout < 300) throw new RuntimeException("paytimeout cannot less 300 seconds");
        if (this.paytimeout > 24 * 60 * 60) throw new RuntimeException("paytimeout cannot greater 1 day");
    }

    public Map<String, String> add(String key, String value) {
        if (this.map == null) this.map = new TreeMap<>();
        this.map.put(key, value);
        return this.map;
    }

    public short getPayway() {
        return payway;
    }

    public void setPayway(short payway) {
        this.payway = payway;
    }

    public long getTrademoney() {
        return trademoney;
    }

    public void setTrademoney(long trademoney) {
        this.trademoney = trademoney;
    }

    public String getTradetitle() {
        return tradetitle;
    }

    public void setTradetitle(String tradetitle) {
        this.tradetitle = tradetitle;
    }

    public String getTradebody() {
        return tradebody;
    }

    public void setTradebody(String tradebody) {
        this.tradebody = tradebody;
    }

    public int getPaytimeout() {
        return paytimeout;
    }

    public void setPaytimeout(int paytimeout) {
        this.paytimeout = paytimeout;
    }

    public String getClientAddr() {
        return clientAddr;
    }

    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }

}
