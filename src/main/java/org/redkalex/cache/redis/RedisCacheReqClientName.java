/*
 *
 */
package org.redkalex.cache.redis;

import java.nio.charset.StandardCharsets;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.*;

/** @author zhangjx */
public class RedisCacheReqClientName extends RedisCacheRequest {

	private final String clientName;

	public RedisCacheReqClientName(String appName, String resourceName) {
		this.clientName = "redkalex" + (Utility.isEmpty(appName) ? "" : ("-" + appName))
				+ (Utility.isEmpty(resourceName) ? "" : (":" + resourceName));
	}

	@Override
	public void writeTo(ClientConnection conn, ByteArray writer) {
		writer.put(mutliLengthBytes(3));

		writer.put(bulkLengthBytes(6));
		writer.put("CLIENT\r\n".getBytes(StandardCharsets.UTF_8));

		writer.put(bulkLengthBytes(7));
		writer.put("SETNAME\r\n".getBytes(StandardCharsets.UTF_8));

		byte[] ns = clientName.getBytes(StandardCharsets.UTF_8);
		writer.put(bulkLengthBytes(ns.length));
		writer.put(ns);
		writer.put(CRLF);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "{CLIENT SETNAME " + clientName + "}";
	}
}
