/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.*;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.source.AbstractDataSource.SourceUrlInfo;
import org.redkale.util.*;

/** @author zhangjx */
public class MySQLCodecTest {

	@Test
	public void run() throws Exception {
		final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
		CountDownLatch cdl = new CountDownLatch(1);
		new WorkThread(null, "", 0, 1, null, null) {
			public void run() {
				SourceUrlInfo info = new SourceUrlInfo();
				info.username = "root";
				info.password = "";
				MyClient client = new MyClient(
						"test",
						null,
						"rw",
						new ClientAddress(new InetSocketAddress("127.0.0.1", 3389)),
						Utility.cpus(),
						16,
						new Properties(),
						info,
						false,
						new Properties());
				MyClientConnection conn =
						(MyClientConnection) client.createClientConnection(asyncGroup.newTCPClientConnection());
				MyClientCodec codec = new MyClientCodec(conn);
				ByteArray array = new ByteArray();
				int[] ints = new int[] {
					0x4a, 0x00, 0x00, 0x00, 0x0a, 0x38, 0x2e, 0x30, 0x2e, 0x32, 0x36, 0x00, 0x16, 0x00, 0x00, 0x00,
					0x77, 0x0b, 0x5e, 0x5c, 0x5d, 0x2d, 0x0f, 0x49, 0x00, 0xff, 0xff, 0xff, 0x02, 0x00, 0xff, 0xcf,
					0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x76, 0x1a, 0x1a, 0x6b, 0x06,
					0x12, 0x1e, 0x27, 0x36, 0x57, 0x3d, 0x3b, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61,
					0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x00
				};
				byte[] data = new byte[ints.length];
				for (int i = 0; i < data.length; i++) {
					data[i] = (byte) ints[i];
				}
				List respResults = new ArrayList();
				try {
					Field respResultsField = ClientCodec.class.getDeclaredField("respResults");
					respResultsField.setAccessible(true);
					respResults = (List) respResultsField.get(codec);
				} catch (Exception e) {
					e.printStackTrace();
				}
				ByteBuffer realbuf;
				boolean bool;
				{
					realbuf = ByteBuffer.wrap(data);
					codec.decodeMessages(realbuf, array);
					bool = respResults.size() > 0;
					respResults.clear();
					System.out.println("had result 0: " + bool);
					Assertions.assertTrue(bool);
				}
				realbuf = ByteBuffer.wrap(data, 0, 1);
				codec.decodeMessages(realbuf, array);
				bool = respResults.size() > 0;
				respResults.clear();
				System.out.println("had result 1: " + bool + ", half = " + codec.halfFrameBytes.length());
				Assertions.assertFalse(bool);

				realbuf = ByteBuffer.wrap(data, 1, 1);
				codec.decodeMessages(realbuf, array);
				bool = respResults.size() > 0;
				respResults.clear();
				System.out.println("had result 2: " + bool + ", half = " + codec.halfFrameBytes.length());
				Assertions.assertFalse(bool);

				realbuf = ByteBuffer.wrap(data, 2, data.length - 2);
				codec.decodeMessages(realbuf, array);
				bool = respResults.size() > 0;
				respResults.clear();
				System.out.println("had result 3: " + bool);
				Assertions.assertTrue(bool);
				cdl.countDown();
			}
		}.start();
		cdl.await();
	}
}
