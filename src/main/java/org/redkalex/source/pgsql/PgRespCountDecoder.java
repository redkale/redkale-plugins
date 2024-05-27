/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import org.redkale.util.ByteArray;

/** @author zhangjx */
public class PgRespCountDecoder extends PgRespDecoder<Integer> {

	public static final PgRespCountDecoder instance = new PgRespCountDecoder();

	private PgRespCountDecoder() {}

	@Override
	public byte messageid() {
		return 'C';
	}

	@Override
	public Integer read(
			PgClientConnection conn,
			ByteBuffer buffer,
			int length,
			ByteArray array,
			PgClientRequest request,
			PgResultSet dataset) {
		int len = length - 5; // 最后字节是(byte)0
		byte b;
		int rows = -1;
		boolean debug = PgsqlDataSource.debug;
		if (debug) {
			array.clear();
		}
		for (int i = 0; i < len; i++) {
			b = buffer.get();
			if (rows != -1) {
				rows = rows * 10 + (b - '0');
			} else if (b == ' ') {
				rows = 0;
			}
			if (debug) {
				array.put(b);
			}
		}
		buffer.get();
		return rows;
	}
}
