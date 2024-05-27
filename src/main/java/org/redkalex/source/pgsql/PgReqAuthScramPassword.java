/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;

/** @author zhangjx */
public class PgReqAuthScramPassword extends PgClientRequest {

	public static final int DEFAULT_NONCE_LENGTH = 24;

	public static final byte[] SCRAM_NAME_BYTES = "SCRAM-SHA-256".getBytes();

	public static final int MIN_ASCII_PRINTABLE_RANGE = 0x21;

	public static final int MAX_ASCII_PRINTABLE_RANGE = 0x7e;

	public static final int EXCLUDED_CHAR = (int) ','; // 0x2c

	protected String username;

	protected String password;

	protected List<String> mechanisms;

	protected SecureRandom secureRandom = new SecureRandom();

	protected String clientNonce;

	protected String reqMessage;

	public PgReqAuthScramPassword(String username, String password, List<String> mechanisms) {
		this.username = username;
		this.password = password;
		this.mechanisms = mechanisms;
		this.clientNonce = createNonce(DEFAULT_NONCE_LENGTH, secureRandom);
		this.reqMessage = "n,,n=" + toSaslName(username) + ",r=" + clientNonce;
	}

	@Override
	public int getType() {
		return REQ_TYPE_AUTH;
	}

	@Override
	public String toString() {
		return "PgReqAuthScramPassword_" + Objects.hashCode(this) + "{username=" + username + ", password=" + password
				+ ", mechanisms=" + mechanisms + ", clientNonce=" + clientNonce + ", reqMessage=" + reqMessage + "}";
	}

	@Override
	public void writeTo(ClientConnection conn, ByteArray array) {
		array.putByte('p');
		int start = array.length();
		array.putInt(0);
		array.put(SCRAM_NAME_BYTES);
		array.putByte(0);
		byte[] keys = reqMessage.getBytes(StandardCharsets.UTF_8);
		array.putInt(keys.length);
		array.put(keys);
		array.putInt(start, array.length() - start);
	}

	protected static String createNonce(int size, SecureRandom random) {
		char[] chars = new char[size];
		int r;
		for (int i = 0; i < size; ) {
			r = random.nextInt(MAX_ASCII_PRINTABLE_RANGE - MIN_ASCII_PRINTABLE_RANGE + 1) + MIN_ASCII_PRINTABLE_RANGE;
			if (r != EXCLUDED_CHAR) {
				chars[i++] = (char) r;
			}
		}
		return new String(chars);
	}

	protected static String toSaslName(String value) {
		if (null == value || value.isEmpty()) return value;
		int nComma = 0, nEqual = 0;
		char[] originalChars = value.toCharArray();
		// Fast path
		for (char c : originalChars) {
			if (',' == c) {
				nComma++;
			} else if ('=' == c) {
				nEqual++;
			}
		}
		if (nComma == 0 && nEqual == 0) return value;

		// Replace chars
		char[] saslChars = new char[originalChars.length + nComma * 2 + nEqual * 2];
		int i = 0;
		for (char c : originalChars) {
			if (',' == c) {
				saslChars[i++] = '=';
				saslChars[i++] = '2';
				saslChars[i++] = 'C';
			} else if ('=' == c) {
				saslChars[i++] = '=';
				saslChars[i++] = '3';
				saslChars[i++] = 'D';
			} else {
				saslChars[i++] = c;
			}
		}

		return new String(saslChars);
	}

	protected static String fromSaslName(String value) {
		if (null == value || value.isEmpty()) return value;

		int nEqual = 0;
		char[] orig = value.toCharArray();

		// Fast path
		for (int i = 0; i < orig.length; i++) {
			if (orig[i] == ',') {
				throw new IllegalArgumentException("Invalid ',' character present in saslName");
			}
			if (orig[i] == '=') {
				nEqual++;
				if (i + 2 > orig.length - 1) {
					throw new IllegalArgumentException("Invalid '=' character present in saslName");
				}
				if (!(orig[i + 1] == '2' && orig[i + 2] == 'C' || orig[i + 1] == '3' && orig[i + 2] == 'D')) {
					throw new IllegalArgumentException(
							"Invalid char '=" + orig[i + 1] + orig[i + 2] + "' found in saslName");
				}
			}
		}
		if (nEqual == 0) return value;

		// Replace characters
		char[] replaced = new char[orig.length - nEqual * 2];

		for (int r = 0, o = 0; r < replaced.length; r++) {
			if ('=' == orig[o]) {
				if (orig[o + 1] == '2' && orig[o + 2] == 'C') {
					replaced[r] = ',';
				} else if (orig[o + 1] == '3' && orig[o + 2] == 'D') {
					replaced[r] = '=';
				}
				o += 3;
			} else {
				replaced[r] = orig[o];
				o += 1;
			}
		}
		return new String(replaced);
	}
}
