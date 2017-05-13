/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.apns;

import org.redkale.util.AnyValue;
import org.redkale.util.Utility;
import org.redkale.convert.json.JsonConvert;
import org.redkale.util.AutoLoad;
import org.redkale.service.Service;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.charset.*;
import java.security.*;
import java.util.Base64;
import java.util.concurrent.*;
import java.util.logging.*;
import javax.annotation.*;
import javax.net.ssl.*;
import org.redkale.util.*;
import org.redkale.service.Local;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
public class ApnsService implements Service {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    @Resource
    protected JsonConvert convert;

    @Resource(name = "APP_HOME")
    protected File home;

    @Resource(name = "property.apns.certpwd")
    protected String apnscertpwd = "1"; //证书的密码

    @Resource(name = "property.apns.certpath") //用来加载证书用
    protected String apnscertpath = "apnspushdev_cert.p12";

    @Resource(name = "property.apns.certbase64") //证书内容的64位编码
    protected String apnscertbase64 = "";

    //测试环境:  gateway.sandbox.push.apple.com
    //正式环境:  gateway.push.apple.com
    @Resource(name = "property.apns.pushaddr") //
    protected String apnspushaddr = "gateway.push.apple.com";

    @Resource(name = "property.apns.pushport") //
    protected int apnspushport = 2195;

    @Resource(name = "property.apns.buffersize") //
    protected int apnsbuffersize = 4096;

    private boolean inited = false;

    private CountDownLatch cdl = new CountDownLatch(1);

    private SSLSocketFactory sslFactory;

    @ResourceListener
    void changeResource(String name, Object newVal, Object oldVal) {
        logger.info("@Resource = " + name + " resource changed:  newVal = " + newVal + ", oldVal = " + oldVal);
        inited = false;
        cdl = new CountDownLatch(1);
        init(null);
    }

    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) this.convert = JsonConvert.root();
        new Thread() {
            {
                setDaemon(true);
                setPriority(Thread.MAX_PRIORITY);
            }

            @Override
            public void run() {
                try {
                    File file = (apnscertpath.indexOf('/') == 0 || apnscertpath.indexOf(':') > 0) ? new File(apnscertpath) : new File(home, "conf/" + apnscertpath);
                    InputStream in;
                    if (apnscertbase64 != null && !apnscertbase64.isEmpty()) {
                        in = new ByteArrayInputStream(Base64.getDecoder().decode(apnscertbase64));
                    } else {
                        in = file.isFile() ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + apnscertpath);
                    }
                    KeyStore ks = KeyStore.getInstance("PKCS12");
                    KeyManagerFactory kf = null;
                    if (in != null) {
                        ks.load(in, apnscertpwd.toCharArray());
                        in.close();
                        kf = KeyManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                        kf.init(ks, apnscertpwd.toCharArray());
                    }

                    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init((KeyStore) null);
                    SSLContext context = SSLContext.getInstance("TLS");
                    context.init(kf == null ? new KeyManager[0] : kf.getKeyManagers(), tmf.getTrustManagers(), null);
                    ApnsService.this.sslFactory = context.getSocketFactory();
                } catch (Exception e) {
                    logger.log(Level.SEVERE, this.getClass().getSimpleName() + " init SSLContext error", e);
                } finally {
                    inited = true;
                    cdl.countDown();
                }
            }
        }.start();
    }

    @Override
    public void destroy(AnyValue conf) {
    }

    private Socket getPushSocket() throws IOException {
        if (!this.inited) {
            try {
                cdl.await();
            } catch (InterruptedException e) {
            }
        }
        Socket pushSocket = sslFactory.createSocket(apnspushaddr, apnspushport);
        pushSocket.setTcpNoDelay(true);
        return pushSocket;
    }

    public void pushApnsMessage(ApnsMessage message) throws IOException {
        final byte[] tokens = Utility.hexToBin(message.getToken().replaceAll("\\s+", ""));
        ByteBuffer buffer = ByteBuffer.allocate(apnsbuffersize);
        buffer.put((byte) 2); //固定命令号 
        buffer.putInt(0); //下面数据的长度

        buffer.put((byte) 1); //token
        buffer.putShort((short) tokens.length);
        buffer.put(tokens);

        buffer.put((byte) 2);  //payload
        final byte[] payload = message.getPayload().toString().getBytes(UTF8);
        buffer.putShort((short) payload.length);
        buffer.put(payload);

        if (message.getIdentifier() > 0) {
            buffer.put((byte) 3);  //Notification identifier
            buffer.putShort((short) 4);
            buffer.putInt(message.getIdentifier());
        }
        if (message.getExpiredate() > 0) {
            buffer.put((byte) 4); //Expiration date
            buffer.putShort((short) 4);
            buffer.putInt(message.getExpiredate());
        }
        buffer.put((byte) 5);  //Priority
        buffer.putShort((short) 1);
        buffer.put((byte) message.getPriority());

        final int pos = buffer.position();
        buffer.position(1);
        buffer.putInt(pos - 5);
        buffer.position(pos);
        buffer.flip();

        Socket socket = getPushSocket();
        socket.getOutputStream().write(buffer.array(), 0, buffer.remaining());
        socket.close();
    }

//    public static void main(String[] args) throws Exception {
//        ApnsService service = new ApnsService();
//        service.apnspushaddr = "gateway.push.apple.com"; //正式环境
//        service.apnscertpwd = "1";
//        service.apnscertpath = "D:/apns.xxx.release.p12";
//        service.init(null);
//
//        final String token = "3ce04256758126f0e8240bed658120b51f78824c2c63b6fb717aa26bc50b28f3";
//        ApnsPayload payload = new ApnsPayload("您有新的消息", "这是消息内容", 0);
//        System.out.println(payload);
//        service.pushApnsMessage(new ApnsMessage(token, payload));
//    }
}
