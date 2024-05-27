/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.convert.json.JsonConvert;

/** @author zhangjx */
public class MyRespHandshakeResultSet extends MyResultSet {

	public int protocolVersion;

	public String serverVersion;

	public String authPluginName;

	public long threadId;

	public byte[] seed;

	public int serverCapabilities;

	public int serverCharsetIndex;

	public int serverStatus;

	public byte[] seed2;

	@Override
	public String toString() {
		return JsonConvert.root().convertTo(this);
	}
}
