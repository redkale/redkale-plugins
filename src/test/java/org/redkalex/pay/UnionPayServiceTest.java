/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import org.redkale.util.Utility;

/** @author zhangjx */
public class UnionPayServiceTest {

    public static void main(String[] args) throws Throwable {
        UnionPayService service = new UnionPayService();
        //        service.createurl = "https://101.231.204.80:5000/gateway/api/appTransReq.do"; //请求支付url
        //        service.queryurl = "https://101.231.204.80:5000/gateway/api/queryTrans.do"; //请求查询url
        //        service.refundurl = "https://101.231.204.80:5000/gateway/api/backTransReq.do"; //请求退款url
        //        service.closeurl = "https://101.231.204.80:5000/gateway/api/backTransReq.do";  //请求关闭url
        //
        //        service.home = new File("D:/Java-Project/RedkalePluginsProject");
        //        service.signcertpath = "acp_test_sign.pfx"; //放在 {APP_HOME}/conf 目录下
        //        service.verifycertpath = "acp_test_verify_sign.cer";//放在 {APP_HOME}/conf 目录下
        //        service.signcertpwd = "000000";
        service.init(null);

        // 支付
        final PayCreatRequest creatRequest = new PayCreatRequest();
        creatRequest.setPayType(Pays.PAYTYPE_UNION);
        creatRequest.setPayno("Redkale100000001");
        creatRequest.setPayMoney(10); // 1毛钱
        creatRequest.setPayTitle("一斤红菜苔");
        creatRequest.setPayBody("一斤红菜苔");
        creatRequest.setClientAddr(Utility.localInetAddress().getHostAddress());
        final PayCreatResponse creatResponse = service.create(creatRequest);
        System.out.println(creatResponse);

        // 查询
        // 请求不能太频繁，否则 You have been added to the blacklist. Please don't do stress testing. TPS could not be greater
        // than 0.5 . BlackList Will be clear at time 00:00
        PayRequest queryRequest = new PayRequest();
        queryRequest.setPayType(Pays.PAYTYPE_UNION);
        queryRequest.setPayno(creatRequest.getPayno());
        // PayQueryResponse queryResponse = service.query(queryRequest);
        // System.out.println(queryResponse);
    }
}
