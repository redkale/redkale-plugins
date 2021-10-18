/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.apns;

/**
 *
 * @author zhangjx
 */
public class ApnsServiceTest {

    public static void main(String[] args) throws Exception {
        ApnsService service = new ApnsService();
        service.apnspushaddr = "gateway.push.apple.com"; //正式环境
        service.apnscertpwd = "1";
        service.apnscertpath = "D:/apns.xxx.release.p12";
        service.init(null);

        final String token = "3ce04256758126f0e8240bed658120b51f78824c2c63b6fb717aa26bc50b28f3";
        ApnsPayload payload = new ApnsPayload("您有新的消息", "这是消息内容", 0);
        System.out.println(payload);
        service.pushApnsMessage(new ApnsMessage(token, payload));
    }
}
