/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.apns;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.logging.*;
import javax.net.ssl.*;
import org.redkale.annotation.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.Comment;
import org.redkale.convert.json.JsonConvert;
import org.redkale.inject.ResourceEvent;
import org.redkale.service.*;
import org.redkale.util.*;
import org.redkale.annotation.ResourceChanged;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@Component
@AutoLoad(false)
public final class ApnsService implements Service {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    @Resource
    protected JsonConvert convert;

    @Resource(name = "APP_HOME")
    protected File home;

    @Resource(name = "apns.certpwd")
    protected String apnscertpwd = "1"; //证书的密码

    @Resource(name = "apns.certpath") //用来加载证书用
    protected String apnscertpath = "apnspushdev_cert.p12";

    @Resource(name = "apns.certbase64") //证书内容的64位编码
    protected String apnscertbase64 = "";

    //测试环境:  gateway.sandbox.push.apple.com
    //正式环境:  gateway.push.apple.com
    @Resource(name = "apns.pushaddr") //
    protected String apnspushaddr = "gateway.push.apple.com";

    @Resource(name = "apns.pushport") //
    protected int apnspushport = 2195;

    @Resource(name = "apns.buffersize") //
    protected int apnsbuffersize = 4096;

    private boolean inited = false;

    private CountDownLatch cdl = new CountDownLatch(1);

    private SSLSocketFactory sslFactory;

    @ResourceChanged
    @Comment("通过配置中心更改配置后的回调")
    void onResourceChanged(ResourceEvent[] events) {
        StringBuilder sb = new StringBuilder();
        for (ResourceEvent event : events) {
            if (event.name().contains("certpwd") || event.name().contains("certbase64")) { //敏感配置不打印日志
                sb.append("@Resource = ").append(event.name()).append(" resource changed");
            } else {
                sb.append("@Resource = ").append(event.name()).append(" resource changed:  newVal = " + event.newValue() + ", oldVal = " + event.oldValue());
            }
        }
        if (sb.length() > 0) {
            logger.log(Level.INFO, sb.toString());
        }
        cdl = new CountDownLatch(1);
        inited = false;
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
        final byte[] payload = message.getPayload().toString().getBytes(StandardCharsets.UTF_8);
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

}
