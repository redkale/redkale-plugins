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
import org.redkale.util.ByteArray;

/**
 *
 * @author zhangjx
 */
public class RedisCacheCodec extends ClientCodec<RedisCacheRequest, RedisCacheResult> {

    protected static final byte TYPE_STRING = '+';  //简单字符串(不包含CRLF)类型

    protected static final byte TYPE_ERROR = '-'; //错误(不包含CRLF)类型

    protected static final byte TYPE_NUMBER = ':'; //整型

    protected static final byte TYPE_BULK = '$';  //块字符串

    protected static final byte TYPE_ARRAY = '*'; //数组

    protected static final Logger logger = Logger.getLogger(RedisCacheCodec.class.getSimpleName());

    protected byte halfFrameCmd;

    protected int halfFrameBulkLength = -10;

    protected int halfFrameArraySize = -10;

    protected int halfFrameArrayIndex; //从0开始

    protected int halfFrameArrayItemLength = -10;

    protected ByteArray halfFrameBytes;

    protected byte frameType;

    protected byte[] frameCursor;

    protected byte[] frameValue;  //(不包含CRLF)

    protected List<byte[]> frameList;  //(不包含CRLF)

    private ByteArray recyclableArray;

    public RedisCacheCodec(ClientConnection connection) {
        super(connection);
    }

    protected ByteArray pollArray(ByteArray array) {
        if (recyclableArray == null) {
            recyclableArray = new ByteArray();
        } else {
            recyclableArray.clear();
        }
        recyclableArray.clear();
        if (array != null) {
            recyclableArray.put(array, 0, array.length());
        }
        return recyclableArray;
    }

    private boolean readFrames(RedisCacheConnection conn, ByteBuffer buffer, ByteArray array) {
//        byte[] dbs = new byte[buffer.remaining()];
//        for (int i = 0; i < dbs.length; i++) {
//            dbs[i] = buffer.get(buffer.position() + i);
//        }
//        ArrayDeque<ClientFuture> deque = (ArrayDeque) responseQueue(conn);
//        logger.log(Level.FINEST, "[" + Utility.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", 原始数据: " + new String(dbs).replace("\r\n", "  ") + ", req=" + deque.getFirst().getRequest());

        array.clear();
        byte type = halfFrameCmd == 0 ? buffer.get() : halfFrameCmd;
        if (halfFrameBytes != null) {
            array.put(halfFrameBytes, 0, halfFrameBytes.length());
        }
        frameType = type;
        if (type == TYPE_STRING || type == TYPE_ERROR || type == TYPE_NUMBER) {
            if (readComplete(buffer, array)) {
                frameValue = array.getBytes();
            } else {
                halfFrameCmd = type;
                halfFrameBytes = pollArray(array);
                return false;
            }
        } else if (type == TYPE_BULK) {
            int bulkLength = halfFrameBulkLength;
            if (bulkLength < -2) {
                if (!readComplete(buffer, array)) { //没有读到bulkLength
                    halfFrameCmd = type;
                    halfFrameBulkLength = -10;
                    halfFrameBytes = pollArray(array);
                    return false;
                }
                bulkLength = Integer.parseInt(array.toString(StandardCharsets.UTF_8));
                array.clear();
            }
            if (bulkLength == -1) {
                frameValue = null;
            } else if (readComplete(buffer, array)) {
                frameValue = array.getBytes();
            } else {
                halfFrameCmd = type;
                halfFrameBulkLength = bulkLength;
                halfFrameBytes = pollArray(array);
                return false;
            }
        } else if (type == TYPE_ARRAY) {
            int arraySize = halfFrameArraySize;
            if (arraySize < -2) {
                if (!readComplete(buffer, array)) { //没有读到arraySize
                    halfFrameCmd = type;
                    halfFrameArraySize = -10;
                    halfFrameArrayIndex = 0;
                    halfFrameArrayItemLength = -10;
                    halfFrameBytes = pollArray(array);
                    return false;
                }
                arraySize = Integer.parseInt(array.toString(StandardCharsets.UTF_8));
                array.clear();
            }
            int arrayIndex = halfFrameArrayIndex;
            for (int i = arrayIndex; i < arraySize; i++) {
                int itemLength = halfFrameArrayItemLength;
                halfFrameArrayItemLength = -10;
                if (itemLength < -2) {
                    if (!readComplete(buffer, array)) { //没有读到bulkLength
                        halfFrameCmd = type;
                        halfFrameArraySize = arraySize;
                        halfFrameArrayIndex = i;
                        halfFrameArrayItemLength = -10;
                        halfFrameBytes = pollArray(array);
                        return false;
                    }
                    byte sign = array.get(0);
                    if (type == TYPE_STRING || type == TYPE_ERROR || type == TYPE_NUMBER) {
                        if (frameList == null) {
                            frameList = new ArrayList<>();
                        }
                        frameList.add(array.getBytes(1, array.length() - 1));
                        array.clear();
                        continue;
                    }
                    itemLength = Integer.parseInt(array.toString(1, StandardCharsets.UTF_8));
                    array.clear();
                    if (sign == TYPE_ARRAY) { //数组中嵌套数组，例如: SCAN、HSCAN
                        frameValue = null;
                        if (frameList != null) {
                            if (frameList.size() == 1) {
                                frameCursor = frameList.get(0);
                            }
                            frameList.clear();
                        }
                        clearHalfFrame();
                        if (itemLength == 0) {
                            return true;
                        }
                        halfFrameCmd = sign;
                        halfFrameArraySize = itemLength;
                        if (!buffer.hasRemaining()) {
                            return false;
                        }
                        return readFrames(conn, buffer, array);
                    }
                }
                int cha = itemLength - array.length();
                if (itemLength == -1) {
                    if (frameList == null) {
                        frameList = new ArrayList<>();
                    }
                    frameList.add(null);
                    array.clear();
                } else if (buffer.remaining() >= cha + 2) {
                    for (int j = 0; j < cha; j++) array.put(buffer.get());
                    buffer.get(); //\r
                    buffer.get(); //\n
                    if (frameList == null) {
                        frameList = new ArrayList<>();
                    }
                    frameList.add(array.getBytes());
                    array.clear();
                } else {
                    while (buffer.hasRemaining()) array.put(buffer.get());
                    halfFrameCmd = type;
                    halfFrameArraySize = arraySize;
                    halfFrameArrayIndex = i;
                    halfFrameArrayItemLength = itemLength;
                    halfFrameBytes = pollArray(array);
                    return false;
                }
            }
        }
        clearHalfFrame();
        return true;
    }

    protected void clearHalfFrame() {
        halfFrameCmd = 0;
        halfFrameBulkLength = -10;
        halfFrameArraySize = -10;
        halfFrameArrayIndex = 0;
        halfFrameArrayItemLength = -10;
        halfFrameBytes = null;
    }

    @Override
    public void decodeMessages(ByteBuffer realbuf, ByteArray array) {
        RedisCacheConnection conn = (RedisCacheConnection) connection;
        if (!realbuf.hasRemaining()) {
            return;
        }
        ByteBuffer buffer = realbuf;
        if (!readFrames(conn, buffer, array)) {
            return;
        }
        //buffer必然包含一个完整的frame数据
        boolean first = true;
        RedisCacheRequest request = null;
        while (first || buffer.hasRemaining()) {
            if (request == null) {
                request = nextRequest();
            }
            if (!first && !readFrames(conn, buffer, array)) {
                break;
            }
            if (frameType == TYPE_ERROR) {
                addMessage(request, new RuntimeException(new String(frameValue, StandardCharsets.UTF_8)));
            } else {
                addMessage(request, conn.pollResultSet(request).prepare(frameType, frameCursor, frameValue, frameList));
            }
            frameType = 0;
            frameCursor = null;
            frameValue = null;
            frameList = null;
            halfFrameCmd = 0;
            halfFrameBytes = null;
            first = false;
            buffer = realbuf;
        }
    }

    protected boolean readComplete(ByteBuffer buffer, ByteArray array) {
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            if (b == '\n') {
                array.removeLastByte(); //移除 \r
                return true;
            }
            array.put(b);
        }
        return false;
    }
}
