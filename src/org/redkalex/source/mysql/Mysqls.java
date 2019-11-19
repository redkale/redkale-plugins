/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.Arrays;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
class Mysqls {

    public static final int MAX_PACKET_SIZE = 256 * 256 * 256 - 1;

    static final String CODE_PAGE_1252 = "Cp1252";

    static final int NULL_LENGTH = ~0;

    static final int COMP_HEADER_LENGTH = 3;

    static final int MIN_COMPRESS_LEN = 50;

    static final int HEADER_LENGTH = 4;

    static final int AUTH_411_OVERHEAD = 33;

    static final int SEED_LENGTH = 20;

    static int maxBufferSize = 65535;

    //------------------------------- SERVER -------------------------------------------------
    static final int SERVER_STATUS_IN_TRANS = 1;

    static final int SERVER_STATUS_AUTOCOMMIT = 2; // Server in auto_commit mode

    static final int SERVER_MORE_RESULTS_EXISTS = 8; // Multi query - next query exists

    static final int SERVER_QUERY_NO_GOOD_INDEX_USED = 16;

    static final int SERVER_QUERY_NO_INDEX_USED = 32;

    static final int SERVER_STATUS_CURSOR_EXISTS = 64;

    static final int SERVER_STATUS_LAST_ROW_SENT = 128; // The server status for 'last-row-sent'

    static final int SERVER_QUERY_WAS_SLOW = 2048;

    //------------------------------- CLIENT -------------------------------------------------
    static final int CLIENT_LONG_PASSWORD = 0x00000001;

    /* new more secure passwords */
    static final int CLIENT_FOUND_ROWS = 0x00000002;

    static final int CLIENT_LONG_FLAG = 0x00000004;

    /* Get all column flags */
    static final int CLIENT_CONNECT_WITH_DB = 0x00000008;

    static final int CLIENT_NO_SCHEMA = 0x00000010;

    static final int CLIENT_COMPRESS = 0x00000020;

    static final int CLIENT_ODBC = 0x00000040;

    /* Can use compression protcol */
    static final int CLIENT_LOCAL_FILES = 0x00000080;

    static final int CLIENT_IGNORE_SPACE = 0x00000100;

    /* Can use LOAD DATA LOCAL */
    static final int CLIENT_PROTOCOL_41 = 0x00000200; // for > 4.1.1

    static final int CLIENT_INTERACTIVE = 0x00000400;

    static final int CLIENT_SSL = 0x00000800;

    static final int CLIENT_IGNORE_SIGPIPE = 0x00001000;

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

    //---------------------------------------------------------------------------------------
    static final String FALSE_SCRAMBLE = "xxxxxxxx";

    static final int MAX_QUERY_SIZE_TO_LOG = 1024; // truncate logging of queries at 1K

    static final int MAX_QUERY_SIZE_TO_EXPLAIN = 1024 * 1024; // don't explain queries above 1MB

    static final int INITIAL_PACKET_SIZE = 1024;

    static final String ZERO_DATE_VALUE_MARKER = "0000-00-00";

    static final String ZERO_DATETIME_VALUE_MARKER = "0000-00-00 00:00:00";

    static final String EXPLAINABLE_STATEMENT = "SELECT";

    static final String[] EXPLAINABLE_STATEMENT_EXTENSION = new String[]{"INSERT", "UPDATE", "REPLACE", "DELETE"};

    //----------------- Buffer ------------------------
    static final short TYPE_ID_ERROR = 0xFF;

    static final short TYPE_ID_EOF = 0xFE;

    /** It has the same signature as EOF, but may be issued by server only during handshake phase * */
    static final short TYPE_ID_AUTH_SWITCH = 0xFE;

    static final short TYPE_ID_LOCAL_INFILE = 0xFB;

    static final short TYPE_ID_OK = 0;

    protected static final int getLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 1 + 2 + length;
        } else if (length < 0x1000000L) {
            return 1 + 3 + length;
        } else {
            return 1 + 8 + length;
        }
    }

//--------------------------- write ------------------------------
    protected static void writeUB2(ByteBuffer buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
    }

    protected static void writeUB2(ByteBufferWriter buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
    }

    protected static void writeUB3(ByteBuffer buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
        buffer.put((byte) (i >>> 16));
    }

    protected static void writeUB3(ByteBufferWriter buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
        buffer.put((byte) (i >>> 16));
    }

    //可以存入一个int类型，表示已经考虑到24-32的含有符号位的8位了。
    protected static void writeInt(ByteBuffer buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
        buffer.put((byte) (i >>> 16));
        buffer.put((byte) (i >>> 24));
    }

    //可以存入一个int类型，表示已经考虑到24-32的含有符号位的8位了。
    protected static void writeInt(ByteBufferWriter buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
        buffer.put((byte) (i >>> 16));
        buffer.put((byte) (i >>> 24));
    }

    protected static void writeFloat(ByteBuffer buffer, float f) {
        writeInt(buffer, Float.floatToIntBits(f));
    }

    protected static void writeFloat(ByteBufferWriter buffer, float f) {
        writeInt(buffer, Float.floatToIntBits(f));
    }

    //如果是存入四个字节，其实是不能使用int的，因为24-32位是有符号位的，所以这里需要使用Long，这样可以保证前32表示的都是值
    protected static void writeUB4(ByteBuffer buffer, long l) {
        buffer.put((byte) (l & 0xff));
        buffer.put((byte) (l >>> 8));
        buffer.put((byte) (l >>> 16));
        buffer.put((byte) (l >>> 24));
    }

    //如果是存入四个字节，其实是不能使用int的，因为24-32位是有符号位的，所以这里需要使用Long，这样可以保证前32表示的都是值
    protected static void writeUB4(ByteBufferWriter buffer, long l) {
        buffer.put((byte) (l & 0xff));
        buffer.put((byte) (l >>> 8));
        buffer.put((byte) (l >>> 16));
        buffer.put((byte) (l >>> 24));
    }

    protected static void writeLong(ByteBuffer buffer, long l) {
        buffer.put((byte) (l & 0xff));
        buffer.put((byte) (l >>> 8));
        buffer.put((byte) (l >>> 16));
        buffer.put((byte) (l >>> 24));
        buffer.put((byte) (l >>> 32));
        buffer.put((byte) (l >>> 40));
        buffer.put((byte) (l >>> 48));
        buffer.put((byte) (l >>> 56));
    }

    protected static void writeLong(ByteBufferWriter buffer, long l) {
        buffer.put((byte) (l & 0xff));
        buffer.put((byte) (l >>> 8));
        buffer.put((byte) (l >>> 16));
        buffer.put((byte) (l >>> 24));
        buffer.put((byte) (l >>> 32));
        buffer.put((byte) (l >>> 40));
        buffer.put((byte) (l >>> 48));
        buffer.put((byte) (l >>> 56));
    }

    protected static void writeDouble(ByteBuffer buffer, double d) {
        writeLong(buffer, Double.doubleToLongBits(d));
    }

    protected static void writeDouble(ByteBufferWriter buffer, double d) {
        writeLong(buffer, Double.doubleToLongBits(d));
    }

    protected static void writeLength(ByteBuffer buffer, long l) {
        if (l < 251) {
            buffer.put((byte) l);
        } else if (l < 0x10000L) {
            buffer.put((byte) 252);
            writeUB2(buffer, (int) l);
        } else if (l < 0x1000000L) {
            buffer.put((byte) 253);
            writeUB3(buffer, (int) l);
        } else {
            buffer.put((byte) 254);
            writeLong(buffer, l);
        }
    }

    protected static void writeLength(ByteBufferWriter buffer, long l) {
        if (l < 251) {
            buffer.put((byte) l);
        } else if (l < 0x10000L) {
            buffer.put((byte) 252);
            writeUB2(buffer, (int) l);
        } else if (l < 0x1000000L) {
            buffer.put((byte) 253);
            writeUB3(buffer, (int) l);
        } else {
            buffer.put((byte) 254);
            writeLong(buffer, l);
        }
    }

    protected static void writeWithNull(ByteBuffer buffer, byte[] src) {
        buffer.put(src);
        buffer.put((byte) 0);
    }

    protected static void writeWithNull(ByteBufferWriter buffer, byte[] src) {
        buffer.put(src);
        buffer.put((byte) 0);
    }

    protected static void writeWithLength(ByteBuffer buffer, byte[] src) {
        int length = src.length;
        if (length < 251) {
            buffer.put((byte) length);
        } else if (length < 0x10000L) {
            buffer.put((byte) 252);
            writeUB2(buffer, length);
        } else if (length < 0x1000000L) {
            buffer.put((byte) 253);
            writeUB3(buffer, length);
        } else {
            buffer.put((byte) 254);
            writeLong(buffer, length);
        }
        buffer.put(src);
    }

    protected static void writeWithLength(ByteBufferWriter buffer, byte[] src) {
        int length = src.length;
        if (length < 251) {
            buffer.put((byte) length);
        } else if (length < 0x10000L) {
            buffer.put((byte) 252);
            writeUB2(buffer, length);
        } else if (length < 0x1000000L) {
            buffer.put((byte) 253);
            writeUB3(buffer, length);
        } else {
            buffer.put((byte) 254);
            writeLong(buffer, length);
        }
        buffer.put(src);
    }

    protected static void writeWithLength(ByteBuffer buffer, byte[] src, byte nullValue) {
        if (src == null) {
            buffer.put(nullValue);
        } else {
            writeWithLength(buffer, src);
        }
    }

    protected static void writeWithLength(ByteBufferWriter buffer, byte[] src, byte nullValue) {
        if (src == null) {
            buffer.put(nullValue);
        } else {
            writeWithLength(buffer, src);
        }
    }
//--------------------------- read ------------------------------

    protected static int readUB2(ByteBuffer buffer) {
        return (buffer.get() & 0xff) | ((buffer.get() & 0xff) << 8);
    }

    protected static int readUB2(ByteBufferReader buffer) {
        return (buffer.get() & 0xff) | ((buffer.get() & 0xff) << 8);
    }

    protected static int readUB3(ByteBuffer buffer) {
        return (buffer.get() & 0xff) | ((buffer.get() & 0xff) << 8) | ((buffer.get() & 0xff) << 16);
    }

    protected static int readUB3(ByteBufferReader buffer) {
        return (buffer.get() & 0xff) | ((buffer.get() & 0xff) << 8) | ((buffer.get() & 0xff) << 16);
    }

    protected static long readUB4(ByteBuffer buffer) {
        return ((long) buffer.get() & 0xff) | (((long) buffer.get() & 0xff) << 8)
            | ((long) (buffer.get() & 0xff) << 16) | ((long) (buffer.get() & 0xff) << 24);
    }

    protected static long readUB4(ByteBufferReader buffer) {
        return ((long) buffer.get() & 0xff) | (((long) buffer.get() & 0xff) << 8)
            | ((long) (buffer.get() & 0xff) << 16) | ((long) (buffer.get() & 0xff) << 24);
    }

    protected static long readLong(ByteBuffer buffer) {
        return ((long) buffer.get() & 0xff) | (((long) buffer.get() & 0xff) << 8)
            | ((long) (buffer.get() & 0xff) << 16) | ((long) (buffer.get() & 0xff) << 24)
            | ((long) (buffer.get() & 0xff) << 32) | ((long) (buffer.get() & 0xff) << 40)
            | ((long) (buffer.get() & 0xff) << 48) | ((long) (buffer.get() & 0xff) << 56);
    }

    protected static long readLong(ByteBufferReader buffer) {
        return ((long) buffer.get() & 0xff) | (((long) buffer.get() & 0xff) << 8)
            | ((long) (buffer.get() & 0xff) << 16) | ((long) (buffer.get() & 0xff) << 24)
            | ((long) (buffer.get() & 0xff) << 32) | ((long) (buffer.get() & 0xff) << 40)
            | ((long) (buffer.get() & 0xff) << 48) | ((long) (buffer.get() & 0xff) << 56);
    }

    protected static long readLength(ByteBuffer buffer) {
        int length = buffer.get() & 0xff;
        switch (length) {
            case 251:
                return NULL_LENGTH;
            case 252:
                return readUB2(buffer);
            case 253:
                return readUB3(buffer);
            case 254:
                return readLong(buffer);
            default:
                return length;
        }
    }

    protected static long readLength(ByteBufferReader buffer) {
        int length = buffer.get() & 0xff;
        switch (length) {
            case 251:
                return NULL_LENGTH;
            case 252:
                return readUB2(buffer);
            case 253:
                return readUB3(buffer);
            case 254:
                return readLong(buffer);
            default:
                return length;
        }
    }

    protected static byte[] readBytesWithLength(ByteBuffer buffer) {
        int length = (int) readLength(buffer);
        if (length == -1) return null;
        if (length <= 0) return new byte[0];
        byte[] bs = new byte[length];
        buffer.get(bs);
        return bs;
    }

    protected static byte[] readBytesWithLength(ByteBufferReader buffer) {
        int length = (int) readLength(buffer);
        if (length == -1) return null;
        if (length <= 0) return new byte[0];
        byte[] bs = new byte[length];
        buffer.get(bs);
        return bs;
    }

    protected static byte[] readBytes(ByteBuffer buffer, byte[] store, int len) {
        byte[] bs = store == null || len > store.length ? new byte[len] : store;
        buffer.get(bs, 0, len);
        return bs == store ? Arrays.copyOf(store, len) : bs;
    }

    protected static byte[] readBytes(ByteBufferReader buffer, byte[] store, int len) {
        byte[] bs = store == null || len > store.length ? new byte[len] : store;
        buffer.get(bs, 0, len);
        return bs == store ? Arrays.copyOf(store, len) : bs;
    }

    protected static byte[] readBytes(ByteBuffer buffer, byte[] store) {
        int i = 0;
        ByteArray array = null;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            if (array != null) {
                array.write(c);
            } else {
                store[i++] = c;
                if (i == store.length) {
                    array = new ByteArray(1024);
                    array.write(store);
                }
            }
            if (!buffer.hasRemaining()) break;
        }
        return array == null ? Arrays.copyOf(store, i) : array.getBytes();
    }

    protected static byte[] readBytes(ByteBufferReader buffer, byte[] store) {
        int i = 0;
        ByteArray array = null;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            if (array != null) {
                array.write(c);
            } else {
                store[i++] = c;
                if (i == store.length) {
                    array = new ByteArray(1024);
                    array.write(store);
                }
            }
            if (!buffer.hasRemaining()) break;
        }
        return array == null ? Arrays.copyOf(store, i) : array.getBytes();
    }

    protected static String readUTF8String(ByteBufferReader buffer, byte[] store) {
        int i = 0;
        ByteArray array = null;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            if (array != null) {
                array.write(c);
            } else {
                store[i++] = c;
                if (i == store.length) {
                    array = new ByteArray(1024);
                    array.write(store);
                }
            }
            if (!buffer.hasRemaining()) break;
        }
        return array == null ? new String(store, 0, i, StandardCharsets.UTF_8) : array.toString(StandardCharsets.UTF_8);
    }

    protected static String readASCIIString(ByteBuffer buffer, byte[] store) {
        int i = 0;
        ByteArray array = null;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            if (array != null) {
                array.write(c);
            } else {
                store[i++] = c;
                if (i == store.length) {
                    array = new ByteArray(1024);
                    array.write(store);
                }
            }
            if (!buffer.hasRemaining()) break;
        }
        return array == null ? new String(store, 0, i, StandardCharsets.US_ASCII) : array.toString(StandardCharsets.US_ASCII);
    }

    protected static String readASCIIString(ByteBuffer buffer, int length) {
        byte[] store = new byte[length];
        buffer.get(store);
        return new String(store, StandardCharsets.US_ASCII);
    }

    protected static String readASCIIString(ByteBufferReader buffer, byte[] store) {
        int i = 0;
        ByteArray array = null;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            if (array != null) {
                array.write(c);
            } else {
                store[i++] = c;
                if (i == store.length) {
                    array = new ByteArray(1024);
                    array.write(store);
                }
            }
            if (!buffer.hasRemaining()) break;
        }
        return array == null ? new String(store, 0, i, StandardCharsets.US_ASCII) : array.toString(StandardCharsets.US_ASCII);
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

    protected static byte[] scramble411(String password, byte[] seeds) {
        if (password == null || password.isEmpty()) return null;
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        byte[] passwordHashStage1 = md.digest(password.getBytes());
        md.reset();

        byte[] passwordHashStage2 = md.digest(passwordHashStage1);
        md.reset();

        md.update(seeds);
        md.update(passwordHashStage2);

        byte[] toBeXord = md.digest();

        int numToXor = toBeXord.length;

        for (int i = 0; i < numToXor; i++) {
            toBeXord[i] = (byte) (toBeXord[i] ^ passwordHashStage1[i]);
        }

        return toBeXord;
    }

    protected static byte[] scrambleCachingSha2(String password, byte[] seeds) {
        if (password == null || password.isEmpty()) return null;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] passwordBytes = password.getBytes();
            int CACHING_SHA2_DIGEST_LENGTH = 32;
            byte[] dig1 = new byte[CACHING_SHA2_DIGEST_LENGTH];
            byte[] dig2 = new byte[CACHING_SHA2_DIGEST_LENGTH];
            byte[] scramble1 = new byte[CACHING_SHA2_DIGEST_LENGTH];
            // SHA2(src) => digest_stage1
            md.update(passwordBytes, 0, passwordBytes.length);
            md.digest(dig1, 0, CACHING_SHA2_DIGEST_LENGTH);
            md.reset();

            // SHA2(digest_stage1) => digest_stage2
            md.update(dig1, 0, dig1.length);
            md.digest(dig2, 0, CACHING_SHA2_DIGEST_LENGTH);
            md.reset();

            // SHA2(digest_stage2, m_rnd) => scramble_stage1
            md.update(dig2, 0, dig1.length);
            md.update(seeds, 0, seeds.length);
            md.digest(scramble1, 0, CACHING_SHA2_DIGEST_LENGTH);

            // XOR(digest_stage1, scramble_stage1) => scramble
            byte[] mysqlScrambleBuff = new byte[CACHING_SHA2_DIGEST_LENGTH];
            xorString(dig1, mysqlScrambleBuff, scramble1, CACHING_SHA2_DIGEST_LENGTH);
            return mysqlScrambleBuff;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void xorString(byte[] from, byte[] to, byte[] scramble, int length) {
        int pos = 0;
        int scrambleLength = scramble.length;
        while (pos < length) {
            to[pos] = (byte) (from[pos] ^ scramble[pos % scrambleLength]);
            pos++;
        }
    }
}
