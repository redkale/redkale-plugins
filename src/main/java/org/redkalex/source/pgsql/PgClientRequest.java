/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.charset.StandardCharsets;
import org.redkale.net.client.ClientRequest;
import org.redkale.source.EntityInfo;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public abstract class PgClientRequest extends ClientRequest {

    public static final int REQ_TYPE_AUTH = 1 << 1;

    public static final int REQ_TYPE_QUERY = 1 << 2;

    public static final int REQ_TYPE_UPDATE = 1 << 3;

    public static final int REQ_TYPE_INSERT = 1 << 4;

    public static final int REQ_TYPE_DELETE = 1 << 5;

    public static final int REQ_TYPE_BATCH = 1 << 6;
    
    public static final int REQ_TYPE_EXTEND_QUERY = (1 << 2) + 1;    //预编译的16进制值都要以1结尾

    public static final int REQ_TYPE_EXTEND_UPDATE = (1 << 3) + 1;   //预编译的16进制值都要以1结尾

    public static final int REQ_TYPE_EXTEND_INSERT = (1 << 4) + 1;   //预编译的16进制值都要以1结尾

    public static final int REQ_TYPE_EXTEND_DELETE = (1 << 5) + 1;   //预编译的16进制值都要以1结尾

    //--------------------------------------------------
    protected EntityInfo info;
    
    public abstract int getType();

    protected static ByteArray writeUTF8String(ByteArray array, String string) {
        array.put(string.getBytes(StandardCharsets.UTF_8));
        array.putByte(0);
        return array;
    }

}
