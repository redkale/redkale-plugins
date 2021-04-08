/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.convert.ConvertDisabled;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class MyOKPacket extends MyPacket {

    public static final byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};

    public static final byte[] AC_OFF = new byte[]{7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};

    public static final short TYPE_ID_ERROR = 0xFF;

    public static final short TYPE_ID_EOF = 0xFE;

    /** It has the same signature as EOF, but may be issued by server only during handshake phase * */
    public static final short TYPE_ID_AUTH_SWITCH = 0xFE;

    public static final short TYPE_ID_LOCAL_INFILE = 0xFB;

    public static final short TYPE_ID_OK = 0x00;

    public int typeid;

    public int vendorCode;

    public String sqlState;

    public String info;

    public long updateCount = -1;

    public long updateID = -1;

    public int warningCount;

    public int statusFlags;

    public MyOKPacket(int len, ByteBufferReader reader, ByteArray array) {
        this.packetLength = len < 1 ? Mysqls.readUB3(reader) : len;
        this.packetIndex = reader.get();
        this.typeid = reader.get() & 0xff;
        if (this.typeid == TYPE_ID_EOF) {
            this.warningCount = Mysqls.readUB2(reader);
            this.statusFlags = Mysqls.readUB2(reader);
        } else if (this.typeid == TYPE_ID_ERROR) {
            this.vendorCode = Mysqls.readUB2(reader);
            byte[] bs = Mysqls.readBytes(reader, array);
            if (bs != null) {
                this.info = new String(bs);
                if (this.info.charAt(0) == '#') {
                    if (this.info.length() > 6) {
                        this.sqlState = this.info.substring(1, 6);
                        this.info = this.info.substring(6);
                        if (this.sqlState.equals("HY000")) {
                            this.sqlState = MyErrorNumbers.mysqlToSqlState(this.vendorCode);
                        }
                    } else {
                        this.sqlState = MyErrorNumbers.mysqlToSqlState(this.vendorCode);
                    }
                } else {
                    this.sqlState = MyErrorNumbers.mysqlToSqlState(this.vendorCode);
                }
            }
        } else {
            //com.mysql.cj.protocol.a.NativeProtocol
            this.updateCount = Mysqls.readLength(reader);
            this.updateID = Mysqls.readLength(reader);
            this.statusFlags = Mysqls.readUB2(reader);
            this.warningCount = Mysqls.readUB2(reader);
            if (reader.hasRemaining()) {
                //buffer.get(); // skips the 'last packet' flag (packet signature)
                //this.vendorCode = MySQLs.readUB2(buffer);
                byte[] bs = Mysqls.readBytes(reader, array);
                if (bs != null) {
                    this.info = new String(bs);
                    if (this.info.charAt(0) == '#') {
                        if (this.info.length() > 6) {
                            this.sqlState = this.info.substring(1, 6);
                            this.info = this.info.substring(6);
                            if (this.sqlState.equals("HY000")) {
                                this.sqlState = MyErrorNumbers.mysqlToSqlState(this.vendorCode);
                            }
                        } else {
                            this.sqlState = MyErrorNumbers.mysqlToSqlState(this.vendorCode);
                        }
                    } else {
                        this.sqlState = MyErrorNumbers.mysqlToSqlState(this.vendorCode);
                    }
                }
            }
        }
    }

    @ConvertDisabled
    public boolean isEOF() {
        return this.typeid == TYPE_ID_EOF && this.packetLength <= 5;
    }

    public boolean isOK() {
        return this.typeid == TYPE_ID_OK;
    }

    public String toMessageString(String defval) {
        if (info == null) return defval;
        return info;
    }

}
