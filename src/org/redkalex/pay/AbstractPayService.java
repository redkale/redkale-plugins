/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.*;
import java.util.stream.Collectors;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.*;

/**
 * 支付抽象类
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
abstract class AbstractPayService implements Service {

    //手机预支付
    public abstract PayPreResponse prepay(PayPreRequest request);

    //手机支付回调
    public abstract PayNotifyResponse notify(PayNotifyRequest request);

    //请求支付
    public abstract PayCreatResponse create(PayCreatRequest request);

    //请求查询
    public abstract PayQueryResponse query(PayRequest request);

    //请求关闭
    public abstract PayResponse close(PayCloseRequest request);

    //请求退款
    public abstract PayRefundResponse refund(PayRefundRequest request);

    //查询退款
    public abstract PayRefundResponse queryRefund(PayRequest request);

    protected abstract String createSign(final PayElement element, Map<String, String> map) throws Exception; //计算签名

    protected abstract boolean checkSign(final PayElement element, Map<String, String> map); //验证签名

    protected final String joinMap(Map<String, String> map) { //map对象转换成 key1=value1&key2=value2&key3=value3
        if (!(map instanceof SortedMap)) map = new TreeMap<>(map);
        return map.entrySet().stream().map((e -> e.getKey() + "=" + e.getValue())).collect(Collectors.joining("&"));
    }

    protected static class PayElement {

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }
}
