/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import org.redkale.convert.ConvertDisabled;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class MySQLOKPacket extends MySQLPacket {

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

    public int statusFlags;

    public int warningCount;

    public MySQLOKPacket(ByteBuffer buffer, byte[] array) {
        Utility.println("MySQLOKorErrorPacket.buffer", buffer);
        this.packetLength = MySQLs.readUB3(buffer);
        this.packetIndex = buffer.get();
        this.typeid = buffer.get() & 0xff;
        if (this.typeid == TYPE_ID_EOF) {
            buffer.get(); // skips the 'last packet' flag (packet signature)
            this.warningCount = MySQLs.readUB2(buffer);
            this.statusFlags = MySQLs.readUB2(buffer);
        } else if (this.typeid == TYPE_ID_ERROR) {
            this.vendorCode = MySQLs.readUB2(buffer);
            byte[] bs = MySQLs.readBytes(buffer, array);
            if (bs != null) {
                this.info = new String(bs);
                if (this.info.charAt(0) == '#') {
                    if (this.info.length() > 6) {
                        this.sqlState = this.info.substring(1, 6);
                        this.info = this.info.substring(6);
                        if (this.sqlState.equals("HY000")) {
                            this.sqlState = MysqlErrorNumbers.mysqlToSqlState(this.vendorCode);
                        }
                    } else {
                        this.sqlState = MysqlErrorNumbers.mysqlToSqlState(this.vendorCode);
                    }
                } else {
                    this.sqlState = MysqlErrorNumbers.mysqlToSqlState(this.vendorCode);
                }
            }
        } else {
            //com.mysql.cj.protocol.a.NativeProtocol
            this.updateCount = MySQLs.readLength(buffer);
            this.updateID = MySQLs.readLength(buffer);
            this.statusFlags = MySQLs.readUB2(buffer);
            this.warningCount = MySQLs.readUB2(buffer);
            if (buffer.hasRemaining()) {
                buffer.get(); // skips the 'last packet' flag (packet signature)
                this.vendorCode = MySQLs.readUB2(buffer);
                byte[] bs = MySQLs.readBytes(buffer, array);
                if (bs != null) {
                    this.info = new String(bs);
                    if (this.info.charAt(0) == '#') {
                        if (this.info.length() > 6) {
                            this.sqlState = this.info.substring(1, 6);
                            this.info = this.info.substring(6);
                            if (this.sqlState.equals("HY000")) {
                                this.sqlState = MysqlErrorNumbers.mysqlToSqlState(this.vendorCode);
                            }
                        } else {
                            this.sqlState = MysqlErrorNumbers.mysqlToSqlState(this.vendorCode);
                        }
                    } else {
                        this.sqlState = MysqlErrorNumbers.mysqlToSqlState(this.vendorCode);
                    }
                }
            }
        }
    }

    public MySQLOKPacket(ByteBufferReader buffer, byte[] array) {
        MySQLs.readUB3((ByteBufferReader) null);
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
