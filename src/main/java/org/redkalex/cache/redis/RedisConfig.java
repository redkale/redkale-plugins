/*
 *
 */
package org.redkalex.cache.redis;

import static org.redkale.source.AbstractCacheSource.*;
import static org.redkale.util.Utility.isEmpty;
import static org.redkale.util.Utility.isNotEmpty;

import java.net.URI;
import java.util.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.util.*;

/** @author zhangjx */
public class RedisConfig {

	private boolean ssl;

	private int db;

	private String username;

	private String password;

	private int maxconns;

	private int pipelines;

	private List<String> addresses;

	public static RedisConfig create(AnyValue conf) {
		RedisConfig result = new RedisConfig();
		int gdb = conf.getIntValue(CACHE_SOURCE_DB, 0);
		String gusername = null;
		String gpassword = conf.getValue(CACHE_SOURCE_PASSWORD);
		int gmaxconns = conf.getIntValue(CACHE_SOURCE_MAXCONNS, Utility.cpus());
		int gpipelines = conf.getIntValue(CACHE_SOURCE_PIPELINES, org.redkale.net.client.Client.DEFAULT_MAX_PIPELINES);
		String nodes = conf.getValue(CACHE_SOURCE_NODES, conf.getValue("url", ""));
		if (Utility.isEmpty(nodes)) {
			throw new RedkaleException("Not found nodes config for redis");
		}
		List<String> addrs = new ArrayList<>();
		for (String url : nodes.replace(',', ';').split(";")) {
			String urluser = null;
			String urlpwd = null;
			String urldb = null;
			String urlmaxconns = null;
			String urlpipelines = null;
			addrs.add(url);
			if (url.startsWith("redis://")) { // 兼容 redis://:1234@127.0.0.1:6379?db=2
				URI uri = URI.create(url);
				String userInfo = uri.getUserInfo();
				if (isEmpty(userInfo)) {
					String authority = uri.getAuthority();
					if (authority != null && authority.indexOf('@') > 0) {
						userInfo = authority.substring(0, authority.indexOf('@'));
					}
				}
				if (isNotEmpty(userInfo)) {
					urlpwd = userInfo;
					if (urlpwd.startsWith(":")) {
						urlpwd = urlpwd.substring(1);
					} else {
						int index = urlpwd.indexOf(':');
						if (index > 0) {
							urluser = urlpwd.substring(0, index);
							urlpwd = urlpwd.substring(index + 1);
						}
					}
				}
				if (isNotEmpty(uri.getQuery())) {
					String[] qrys = uri.getQuery().split("&|=");
					for (int i = 0; i < qrys.length; i += 2) {
						if (CACHE_SOURCE_USER.equals(qrys[i])) {
							urluser = i == qrys.length - 1 ? "" : qrys[i + 1];
						} else if (CACHE_SOURCE_PASSWORD.equals(qrys[i])) {
							urlpwd = i == qrys.length - 1 ? "" : qrys[i + 1];
						} else if (CACHE_SOURCE_DB.equals(qrys[i])) {
							urldb = i == qrys.length - 1 ? "" : qrys[i + 1];
						} else if (CACHE_SOURCE_MAXCONNS.equals(qrys[i])) {
							urlmaxconns = i == qrys.length - 1 ? "" : qrys[i + 1];
						} else if (CACHE_SOURCE_PIPELINES.equals(qrys[i])) {
							urlpipelines = i == qrys.length - 1 ? "" : qrys[i + 1];
						}
					}
				}
				if (isNotEmpty(urluser)) {
					gusername = urluser;
				}
				if (isNotEmpty(urlpwd)) {
					gpassword = urlpwd;
				}
				if (isNotEmpty(urlmaxconns)) {
					gmaxconns = Integer.parseInt(urlmaxconns);
				}
				if (isNotEmpty(urlpipelines)) {
					gpipelines = Integer.parseInt(urlpipelines);
				}
				if (isNotEmpty(urldb)) {
					gdb = Integer.parseInt(urldb);
				}
			}
		}
		result.ssl = nodes.startsWith("rediss://");
		result.db = gdb;
		if (isNotEmpty(gusername)) {
			result.username = gusername;
		}
		if (isNotEmpty(gpassword)) {
			result.password = gpassword;
		}
		result.maxconns = gmaxconns;
		result.pipelines = gpipelines;
		result.addresses = addrs;
		return result;
	}

	public boolean isSsl() {
		return ssl;
	}

	public void setSsl(boolean ssl) {
		this.ssl = ssl;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getDb() {
		return db;
	}

	public void setDb(int db) {
		this.db = db;
	}

	public int getMaxconns() {
		return maxconns;
	}

	public int getMaxconns(int min) {
		return Math.max(maxconns, min);
	}

	public void setMaxconns(int maxconns) {
		this.maxconns = maxconns;
	}

	public int getPipelines() {
		return pipelines;
	}

	public void setPipelines(int pipelines) {
		this.pipelines = pipelines;
	}

	public List<String> getAddresses() {
		return addresses;
	}

	public void setAddresses(List<String> addresses) {
		this.addresses = addresses;
	}

	@Override
	public String toString() {
		return JsonConvert.root().convertTo(this);
	}
}
