/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
class MySQLs {

    static final String CODE_PAGE_1252 = "Cp1252";

    static final int NULL_LENGTH = ~0;

    static final int COMP_HEADER_LENGTH = 3;

    static final int MIN_COMPRESS_LEN = 50;

    static final int HEADER_LENGTH = 4;

    static final int AUTH_411_OVERHEAD = 33;

    static final int SEED_LENGTH = 20;

    static int maxBufferSize = 65535;

    static final int CLIENT_LONG_PASSWORD = 0x00000001;

    /* new more secure passwords */
    static final int CLIENT_FOUND_ROWS = 0x00000002;

    static final int CLIENT_LONG_FLAG = 0x00000004;

    /* Get all column flags */
    protected static final int CLIENT_CONNECT_WITH_DB = 0x00000008;

    static final int CLIENT_COMPRESS = 0x00000020;

    /* Can use compression protcol */
    static final int CLIENT_LOCAL_FILES = 0x00000080;

    /* Can use LOAD DATA LOCAL */
    static final int CLIENT_PROTOCOL_41 = 0x00000200; // for > 4.1.1

    static final int CLIENT_INTERACTIVE = 0x00000400;

    protected static final int CLIENT_SSL = 0x00000800;

    static final int CLIENT_TRANSACTIONS = 0x00002000; // Client knows about transactions

    static final int CLIENT_RESERVED = 0x00004000; // for 4.1.0 only

    static final int CLIENT_SECURE_CONNECTION = 0x00008000;

    static final int CLIENT_MULTI_STATEMENTS = 0x00010000; // Enable/disable multiquery support

    static final int CLIENT_MULTI_RESULTS = 0x00020000; // Enable/disable multi-results

    static final int CLIENT_PLUGIN_AUTH = 0x00080000;

    static final int CLIENT_CONNECT_ATTRS = 0x00100000;

    static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;

    static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORD = 0x00400000;

    static final int CLIENT_SESSION_TRACK = 0x00800000;

    static final int CLIENT_DEPRECATE_EOF = 0x01000000;

    static final int SERVER_STATUS_IN_TRANS = 1;

    static final int SERVER_STATUS_AUTOCOMMIT = 2; // Server in auto_commit mode

    static final int SERVER_MORE_RESULTS_EXISTS = 8; // Multi query - next query exists

    static final int SERVER_QUERY_NO_GOOD_INDEX_USED = 16;

    static final int SERVER_QUERY_NO_INDEX_USED = 32;

    static final int SERVER_QUERY_WAS_SLOW = 2048;

    static final int SERVER_STATUS_CURSOR_EXISTS = 64;

    static final String FALSE_SCRAMBLE = "xxxxxxxx";

    static final int MAX_QUERY_SIZE_TO_LOG = 1024; // truncate logging of queries at 1K

    static final int MAX_QUERY_SIZE_TO_EXPLAIN = 1024 * 1024; // don't explain queries above 1MB

    static final int INITIAL_PACKET_SIZE = 1024;

    static final String ZERO_DATE_VALUE_MARKER = "0000-00-00";

    static final String ZERO_DATETIME_VALUE_MARKER = "0000-00-00 00:00:00";

    static final String EXPLAINABLE_STATEMENT = "SELECT";

    static final String[] EXPLAINABLE_STATEMENT_EXTENSION = new String[]{"INSERT", "UPDATE", "REPLACE", "DELETE"};

    protected static int readInt(ByteBuffer buffer) {
        return (buffer.get() & 0xff) | ((buffer.get() & 0xff) << 8);
    }

    protected static long readLong(ByteBuffer buffer) {
        return ((long) buffer.get() & 0xff) | (((long) buffer.get() & 0xff) << 8)
            | ((long) (buffer.get() & 0xff) << 16) | ((long) (buffer.get() & 0xff) << 24);
    }

    protected static long readLong(ByteBufferReader buffer) {
        return ((long) buffer.get() & 0xff) | (((long) buffer.get() & 0xff) << 8)
            | ((long) (buffer.get() & 0xff) << 16) | ((long) (buffer.get() & 0xff) << 24);
    }

    protected static String readUTF8String(ByteBuffer buffer, byte[] store) {
        int i = 0;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            store[i++] = c;
        }
        return new String(store, 0, i, StandardCharsets.UTF_8);
    }

    protected static String readUTF8String(ByteBuffer buffer, int length) {
        byte[] store = new byte[length];
        buffer.get(store);
        return new String(store, StandardCharsets.UTF_8);
    }

    protected static String readUTF8String(ByteBufferReader buffer, byte[] store) {
        int i = 0;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            store[i++] = c;
        }
        return new String(store, 0, i, StandardCharsets.UTF_8);
    }

    protected static String readUTF8String(ByteBufferReader buffer, int length) {
        byte[] store = new byte[length];
        buffer.get(store);
        return new String(store, StandardCharsets.UTF_8);
    }

    protected static String readASCIIString(ByteBuffer buffer, byte[] store) {
        int i = 0;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            store[i++] = c;
        }
        return new String(store, 0, i, StandardCharsets.US_ASCII);
    }

    protected static String readASCIIString(ByteBuffer buffer, int length) {
        byte[] store = new byte[length];
        buffer.get(store);
        return new String(store, StandardCharsets.US_ASCII);
    }

    protected static String readASCIIString(ByteBufferReader buffer, byte[] store) {
        int i = 0;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            store[i++] = c;
        }
        return new String(store, 0, i, StandardCharsets.US_ASCII);
    }

    protected static String readASCIIString(ByteBufferReader buffer, int length) {
        byte[] store = new byte[length];
        buffer.get(store);
        return new String(store, StandardCharsets.US_ASCII);
    }

    protected static ByteBuffer writeUTF8String(ByteBuffer buffer, String string) {
        buffer.put(string.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        return buffer;
    }

    protected static ByteBufferWriter writeUTF8String(ByteBufferWriter buffer, String string) {
        buffer.put(string.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        return buffer;
    }
}
