/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;
import org.redkale.net.client.*;
import org.redkale.util.*;

/** @author zhangjx */
public class RedisCacheCodec extends ClientCodec<RedisCacheRequest, RedisCacheResult> {

	protected static final byte TYPE_BULK = '$'; // 字符串块类型, 例如：$6\r\n\abcdef\r\n，NULL字符串：$-1\r\n

	protected static final byte TYPE_MULTI = '*'; // 数组，紧接的数字为数组长度

	protected static final byte TYPE_STRING = '+'; // 字符串值类型，字符串以\r\n结尾, 例如：+OK\r\n

	protected static final byte TYPE_ERROR = '-'; // 错误字符串类型，字符串以\r\n结尾, 例如：-ERR unknown command 'red'\r\n

	protected static final byte TYPE_NUMBER = ':'; // 整型， 例如：:2\r\n

	private static final Logger logger = Logger.getLogger(RedisCacheCodec.class.getSimpleName());

	private ByteArray halfFrameBytes;

	private int halfFrameBulkLength = Integer.MIN_VALUE;

	private int halfFrameMultiSize = Integer.MIN_VALUE;

	private int halfFrameMultiItemIndex; // 从0开始

	private byte halfFrameMultiItemType;

	private int halfFrameMultiItemLength = Integer.MIN_VALUE;

	private byte frameType;

	private byte[] frameCursor;

	private byte[] frameValue;

	private List<byte[]> frameList;

	private ByteArray recyclableArray;

	public RedisCacheCodec(ClientConnection connection) {
		super(connection);
	}

	private ByteArray pollArray(ByteArray array) {
		if (recyclableArray == null) {
			recyclableArray = new ByteArray();
		}
		recyclableArray.clear();
		if (array != null) {
			recyclableArray.put(array);
		}
		return recyclableArray;
	}

	private boolean readFrames(RedisCacheConnection conn, ByteBuffer buffer, ByteArray array) {
		//        byte[] dbs = new byte[buffer.remaining()];
		//        for (int i = 0; i < dbs.length; i++) {
		//            dbs[i] = buffer.get(buffer.position() + i);
		//        }
		//        (System. out).println("[" + Utility.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " +
		// conn + ", 原始数据: " + new String(dbs).replace("\r\n", "  "));

		array.clear();
		if (this.frameType == 0) {
			this.frameType = buffer.get();
		} else if (halfFrameBytes != null) {
			array.put(halfFrameBytes);
			halfFrameBytes = null;
		}
		if (frameType == TYPE_STRING || frameType == TYPE_ERROR || frameType == TYPE_NUMBER) {
			if (!readComplete(buffer, array)) {
				halfFrameBytes = pollArray(array);
				return false;
			}
			frameValue = array.getBytes();
		} else if (frameType == TYPE_BULK) {
			if (halfFrameBulkLength == Integer.MIN_VALUE) {
				if (!readComplete(buffer, array)) { // 没有读到bulkLength
					halfFrameBytes = pollArray(array);
					return false;
				}
				halfFrameBulkLength = readInt(array);
				array.clear();
			}
			if (halfFrameBulkLength == -1) {
				frameValue = null;
			} else {
				int expect = halfFrameBulkLength + 2 - array.length();
				if (buffer.remaining() < expect) {
					array.put(buffer);
					halfFrameBytes = pollArray(array);
					return false;
				}
				array.put(buffer, expect);
				array.removeLastByte(); // 移除\n
				array.removeLastByte(); // 移除\r
				frameValue = array.getBytes();
			}
		} else if (frameType == TYPE_MULTI) {
			int size = halfFrameMultiSize;
			if (size == Integer.MIN_VALUE) {
				if (!readComplete(buffer, array)) { // 没有读到bulkLength
					halfFrameBytes = pollArray(array);
					return false;
				}
				size = readInt(array);
				halfFrameMultiSize = size;
				array.clear();
				frameValue = null;
			}
			if (frameList == null) {
				frameList = new ArrayList<>();
			}
			if (size > 0) {
				int index = halfFrameMultiItemIndex;
				for (int i = index; i < size; i++) {
					if (!buffer.hasRemaining()) {
						return false;
					}
					if (halfFrameMultiItemType == 0) {
						halfFrameMultiItemType = buffer.get();
					}
					halfFrameMultiItemIndex = i;
					final byte itemType = halfFrameMultiItemType;
					if (itemType == TYPE_STRING || itemType == TYPE_ERROR || itemType == TYPE_NUMBER) {
						if (!readComplete(buffer, array)) {
							halfFrameBytes = pollArray(array);
							return false;
						}
						frameList.add(array.getBytes());
					} else if (itemType == TYPE_BULK) {
						if (halfFrameMultiItemLength == Integer.MIN_VALUE) {
							if (!readComplete(buffer, array)) { // 没有读到bulkLength
								halfFrameBytes = pollArray(array);
								return false;
							}
							halfFrameMultiItemLength = readInt(array);
							array.clear();
						}
						if (halfFrameMultiItemLength == -1) {
							frameList.add(null);
						} else {
							int expect = halfFrameMultiItemLength + 2 - array.length();
							if (buffer.remaining() < expect) {
								array.put(buffer);
								halfFrameBytes = pollArray(array);
								return false;
							}
							array.put(buffer, expect);
							array.removeLastByte(); // 移除\n
							array.removeLastByte(); // 移除\r
							frameList.add(array.getBytes());
						}
					} else if (itemType == TYPE_MULTI) { // 数组中嵌套数组，例如: SCAN、HSCAN
						if (size == 2 && frameList != null && frameList.size() == 1) {
							// 读游标 数据例如: *2  $1  0  *4  $4  key1  $2  10  $4  key2  $2  30
							frameCursor = frameList.get(0);
							frameList.clear();
							clearHalfFrame();
							return readFrames(conn, buffer, array);
						} else {
							throw new RedkaleException("Not support multi type in array data");
						}
					}
					halfFrameMultiItemType = 0;
					halfFrameMultiItemLength = Integer.MIN_VALUE;
					array.clear();
				}
			}
		}
		return true;
	}

	private void clearHalfFrame() {
		halfFrameBytes = null;
		halfFrameBulkLength = Integer.MIN_VALUE;
		halfFrameMultiSize = Integer.MIN_VALUE;
		halfFrameMultiItemLength = Integer.MIN_VALUE;
		halfFrameMultiItemIndex = 0; // 从0开始
		halfFrameMultiItemType = 0;
	}

	@Override
	public void decodeMessages(ByteBuffer realbuf, ByteArray array) {
		RedisCacheConnection conn = (RedisCacheConnection) connection;
		if (!realbuf.hasRemaining()) {
			return;
		}
		ByteBuffer buffer = realbuf;

		while (buffer.hasRemaining()) {
			if (!readFrames(conn, buffer, array)) {
				break;
			}
			RedisCacheRequest request = nextRequest();
			if (frameType == TYPE_ERROR) {
				addMessage(request, new RedkaleException(new String(frameValue, StandardCharsets.UTF_8)));
			} else {
				addMessage(request, conn.pollResultSet(request).prepare(frameType, frameCursor, frameValue, frameList));
			}
			frameType = 0;
			frameCursor = null;
			frameValue = null;
			frameList = null;
			clearHalfFrame();
			buffer = realbuf;
		}
	}

	protected RedisCacheRequest nextRequest() {
		return super.nextRequest();
	}

	private boolean readComplete(ByteBuffer buffer, ByteArray array) {
		while (buffer.hasRemaining()) {
			byte b = buffer.get();
			if (b == '\n') {
				array.removeLastByte(); // 移除 \r
				return true;
			}
			array.put(b);
		}
		return false;
	}

	private int readInt(ByteArray array) {
		String val = array.toString(StandardCharsets.ISO_8859_1);
		if (val.length() == 1 && val.charAt(0) == '0') {
			return 0;
		}
		if (val.length() == 2 && val.charAt(0) == '-' && val.charAt(1) == '1') {
			return -1;
		}
		return Integer.parseInt(val);
	}
}
