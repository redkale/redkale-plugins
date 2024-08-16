/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.charset.StandardCharsets;
import org.redkale.net.WorkThread;
import org.redkale.net.client.ClientRequest;
import org.redkale.source.EntityInfo;
import org.redkale.util.ByteArray;

/** @author zhangjx */
public abstract class PgClientRequest extends ClientRequest {

    public static final int REQ_TYPE_AUTH = 1 << 1;

    public static final int REQ_TYPE_QUERY = 1 << 2;

    public static final int REQ_TYPE_UPDATE = 1 << 3;

    public static final int REQ_TYPE_INSERT = 1 << 4;

    public static final int REQ_TYPE_DELETE = 1 << 5;

    public static final int REQ_TYPE_BATCH = 1 << 6;

    public static final int REQ_TYPE_EXTEND_QUERY = (1 << 2) + 1; // 预编译的16进制值都要以1结尾

    public static final int REQ_TYPE_EXTEND_UPDATE = (1 << 3) + 1; // 预编译的16进制值都要以1结尾

    public static final int REQ_TYPE_EXTEND_INSERT = (1 << 4) + 1; // 预编译的16进制值都要以1结尾

    public static final int REQ_TYPE_EXTEND_DELETE = (1 << 5) + 1; // 预编译的16进制值都要以1结尾

    private static final byte[] SYNC_BYTES =
            new ByteArray(128).putByte('S').putInt(4).getBytes();

    private static final byte[] EXECUTE_BYTES = new ByteArray(128)
            .putByte('E')
            .putInt(4 + 1 + 4)
            .putByte(0)
            .putInt(0)
            .getBytes();

    // --------------------------------------------------
    protected EntityInfo info;

    protected int syncCount;

    public abstract int getType();

    public boolean isExtendType() {
        return (getType() & 0x1) == 1;
    }

    public int getSyncCount() {
        return syncCount;
    }

    @Override
    protected void prepare() {
        super.prepare();
        this.syncCount = 0;
    }

    @Override
    protected boolean recycle() {
        return super.recycle();
    }

    @Override
    protected boolean isCompleted() {
        return super.isCompleted();
    }

    protected final void writeExecute(ByteArray array, int fetchSize) { // EXECUTE
        if (fetchSize < 1) {
            array.put(EXECUTE_BYTES);
        } else {
            array.putByte('E');
            array.putInt(4 + 1 + 4);
            array.putByte(0); // portal 要执行的入口的名字(空字符串选定未命名的入口)。
            array.putInt(fetchSize); // 要返回的最大行数，如果入口包含返回行的查询(否则忽略)。零标识"没有限制"。
        }
    }

    protected final void writeSync(ByteArray array) { // SYNC
        array.put(SYNC_BYTES);
        this.syncCount++;
    }

    protected static ByteArray writeUTF8String(ByteArray array, String string) {
        array.put(string.getBytes(StandardCharsets.UTF_8));
        array.putByte(0);
        return array;
    }

    protected WorkThread getWorkThread() {
        return workThread;
    }

    public PgClientRequest reuse() {
        return this;
    }
}
