/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.lang.reflect.Field;
import java.net.*;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.redkale.net.AsyncIOGroup;
import org.redkale.net.client.*;
import org.redkale.source.AbstractDataSource.SourceUrlInfo;
import org.redkale.util.Utility;

/** @author zhangjx */
public class PgClientTest {

	public static void main(String[] args) throws Throwable {
		final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 2);
		asyncGroup.start();
		SourceUrlInfo info = new SourceUrlInfo();
		info.username = "postgres";
		info.password = "1234";
		info.database = "hello_world";
		SocketAddress address = new InetSocketAddress("127.0.0.1", 5432);
		Properties prop = new Properties();
		prop.put("preparecache", "true");
		final PgClient client =
				new PgClient("test", asyncGroup, "rw", new ClientAddress(address), 2, 16, false, prop, info);
		PgReqQuery showReqQuery = new PgReqQuery();
		showReqQuery.prepare("show all");
		CompletableFuture.allOf(client.sendAsync(showReqQuery), client.sendAsync(showReqQuery))
				.join();
		Field connArrayField = Client.class.getDeclaredField("connArray");
		connArrayField.setAccessible(true);
		ClientConnection[] connArray = (ClientConnection[]) connArrayField.get(client);
		for (ClientConnection conn : connArray) {
			//    conn.writeCounter.set(0);
			//    conn.readCounter.set(0);
		}
		Utility.sleep(800);
		System.out.println(
				"\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n==================================开始==================================");
		final int count = 200;
		CountDownLatch cdl2 = new CountDownLatch(count);
		CountDownLatch cdl = new CountDownLatch(count);
		AtomicLong s = new AtomicLong();
		AtomicBoolean show = new AtomicBoolean();
		AtomicBoolean show2 = new AtomicBoolean();
		for (int i = 0; i < count; i++) {
			new Thread() {
				public void run() {
					cdl.countDown();
					try {
						cdl.await();
						s.compareAndSet(0, System.currentTimeMillis());
						PgReqQuery req = new PgReqQuery();
						req.prepare("show all");
						for (int j = 0; j < 10; j++) {
							PgResultSet reset = client.sendAsync(req).join();
							// System.out.println(client.conns[0]);
							// System.out.println(client.connflags[0]);
							boolean empty = true;
							if (s.get() > 0 && show.compareAndSet(false, true)) {
								while (reset.next()) {
									System.out.println("第一次: " + reset.getObject(1) + ": " + reset.getObject(2));
								}
							} else if (s.get() > 0 && show2.compareAndSet(false, true)) {
								while (reset.next()) {
									System.out.println("第二次: " + reset.getObject(1) + ": " + reset.getObject(2));
								}
							} else {
								while (reset.next()) {
									reset.getObject(1);
									empty = false;
								}
							}
							if (s.get() > 0 && empty) {
								System.out.println("没有结果： " + reset.getUpdateEffectCount());
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					cdl2.countDown();
				}
			}.start();
		}
		cdl2.await();
		System.out.println("耗时: " + (System.currentTimeMillis() - s.get()) + " ms");
	}
}
