/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.*;
import static org.redkalex.source.mysql.Mysqls.*;

/**
 *
 * @author zhangjx
 */
public class MyReqExtended extends MyClientRequest {

    protected int type;

    protected int fetchSize;

    protected Attribute[] attrs;

    protected String sql;

    protected Object[][] parameters;

    protected int sendBindCount;

    protected boolean sendPrepare;

    protected boolean finds;

    public MyReqExtended() {
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "_" + Objects.hashCode(this) + "{sql = '" + sql + "', sendPrepare = " + sendPrepare + ", type = " + getType() + ", params = " + (parameters != null && parameters.length > 10 ? ("size " + parameters.length) : JsonConvert.root().convertTo(parameters)) + "}";
    }

    @Override
    public int getType() {
        return type;
    }

    public <T> void prepare(int type, String sql, int fetchSize, final Attribute<T, Serializable>[] attrs, final Object[]... parameters) {
        super.prepare();
        this.type = type;
        this.sql = sql;
        this.fetchSize = fetchSize;
        this.attrs = attrs;
        this.parameters = parameters;
    }

    @Override
    public MyReqExtended reuse() {
        this.sendPrepare = false;
        this.sendBindCount = 0;
        return this;
    }

    @Override
    public boolean isCompleted() {
        return !sendPrepare;
    }

    public int getBindCount() {
        return sendBindCount;
    }

    @Override
    public void accept(ClientConnection conn, ByteArray array) {
        MyClientConnection myconn = (MyClientConnection) conn;
        AtomicBoolean prepared = myconn.getPrepareFlag(sql);
        Logger logger = myconn.logger();
        if (prepared.get()) {
            this.sendPrepare = false; //此对象会复用，第二次调用
            if (MysqlDataSource.debug) logger.log(Level.FINEST, "[" + Utility.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + " 写入请求包 writeBind: " + this);
            writeBind(myconn, array);
        } else {
            this.sendPrepare = true;
            prepared.set(true);
            if (MysqlDataSource.debug) logger.log(Level.FINEST, "[" + Utility.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + " 写入请求包 writePrepare: " + this);
            writePrepare(myconn, array);
        }
    }

    protected void writePrepare(MyClientConnection conn, ByteArray array) {
        byte[] sqlbytes = sql.getBytes(StandardCharsets.UTF_8);
        Mysqls.writeUB3(array, 1 + sqlbytes.length);
        array.put(packetIndex);
        array.put(COM_STMT_PREPARE);
        array.put(sqlbytes);
    }

    //https://dev.mysql.com/doc/internals/en/com-stmt-execute-response.html
    protected void writeBind(MyClientConnection conn, ByteArray array) {
        if (parameters != null && parameters.length > 0) {
            MyPrepareDesc prepareDesc = conn.getPrepareDesc(sql);
            for (Object[] params : parameters) {
                writeSingleBind(conn, array, prepareDesc, params);
            }
        } else {
            final int startPos = array.length();
            Mysqls.writeUB3(array, 0);
            array.putByte(packetIndex);
            array.put(COM_STMT_EXECUTE);
            Mysqls.writeUB4(array, conn.getStatementIndex(sql));
            array.putByte(0);   //not OPEN_CURSOR_FLAG placeholder for flags
            Mysqls.writeInt(array, 1); // iteration count, always 1
            Mysqls.writeUB3(array, startPos, array.length() - startPos - 4);
        }
    }

    protected void writeSingleBind(MyClientConnection conn, ByteArray array, MyPrepareDesc prepareDesc, Object[] params) {
        final int startPos = array.length();
        Mysqls.writeUB3(array, 0);
        array.putByte(packetIndex);

        array.put(COM_STMT_EXECUTE);
        Mysqls.writeUB4(array, conn.getStatementIndex(sql));
        array.putByte(0);   //not OPEN_CURSOR_FLAG placeholder for flags
        Mysqls.writeInt(array, 1); // iteration count, always 1

        int numOfParams = prepareDesc.numberOfParameters;
        if (numOfParams > 0) {
            int bitmapLength = (numOfParams + 7) / 8;
            byte[] nullBitmap = new byte[bitmapLength];
            int nullPos = array.length();
            // write a dummy bitmap first
            array.put(nullBitmap);  //占位
            boolean sendTypeToServer = true;
            array.putByte(sendTypeToServer ? 1 : 0); //In case if buffers (type) altered, indicate to server
            //MyRowColumn[] paramColumns = entity.prepare.paramDescs.columns;
            for (int i = 0; i < numOfParams; i++) {
                Object param = params[i];
                if (params[i] == null) {
                    nullBitmap[i / 8] |= (1 << (i & 7));
                }
                if (sendTypeToServer) {
                    int t = MysqlType.getTypeFromObject(param);
                    array.putByte(t);
                    array.putByte(0); // parameter flag: signed
                    //if(t != paramColumns[i].type) System.out.println(i + ", t = " + t + ", type = " +paramColumns[i].type);
                }
            }
            array.put(nullPos, nullBitmap); //重新赋值
            for (int i = 0; i < numOfParams; i++) {
                Object param = params[i];
                int t = MysqlType.getTypeFromObject(param);
                if (param == null) continue;
                MysqlType.writePrepareParam(array, t, param);
            }
        }
        Mysqls.writeUB3(array, startPos, array.length() - startPos - 4);
        sendBindCount++;
    }
}
