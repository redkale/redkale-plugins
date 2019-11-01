/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class MySQLOKorErrorPacket extends MySQLPacket {

    private static final byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};

    private static final byte[] AC_OFF = new byte[]{7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};

    private static final int STATUSCODE_OK = 0x00;

    public int statusCode;

    public int vendorCode;

    public String sqlState;

    public String message;

    public long affectedRows;

    public long insertId;

    public int serverStatus;

    public int warningCount;

    public MySQLOKorErrorPacket(ByteBuffer buffer, byte[] array) {
        Utility.println("MySQLOKorErrorPacket.buffer", buffer);
        packetLength = MySQLs.readUB3(buffer);
        packetId = buffer.get();
        this.statusCode = buffer.get() & 0xff;
        if (this.statusCode == 0xff) {
            this.vendorCode = MySQLs.readUB2(buffer);
            byte[] bs = MySQLs.readBytes(buffer, array);
            if (bs != null) {
                this.message = new String(bs);
                if (this.message.charAt(0) == '#') {
                    if (this.message.length() > 6) {
                        this.sqlState = this.message.substring(1, 6);
                        this.message = this.message.substring(6);
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
            affectedRows = MySQLs.readLength(buffer);
            insertId = MySQLs.readLength(buffer);
            serverStatus = MySQLs.readUB2(buffer);
            warningCount = MySQLs.readUB2(buffer);
            if (buffer.hasRemaining()) this.message = new String(MySQLs.readBytes(buffer, array));
        }
    }

    public MySQLOKorErrorPacket(ByteBufferReader buffer, byte[] array) {
        packetLength = MySQLs.readUB3(buffer);
        packetId = buffer.get();
        this.statusCode = buffer.get() & 0xff;
        if (this.statusCode == 0xff) {
            this.vendorCode = MySQLs.readUB2(buffer);
            byte[] bs = MySQLs.readBytes(buffer, array);
            if (bs != null) {
                this.message = new String(bs);
                if (this.message.charAt(0) == '#') {
                    if (this.message.length() > 6) {
                        this.sqlState = this.message.substring(1, 6);
                        this.message = this.message.substring(6);
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
            affectedRows = MySQLs.readLength(buffer);
            insertId = MySQLs.readLength(buffer);
            serverStatus = MySQLs.readUB2(buffer);
            warningCount = MySQLs.readUB2(buffer);
            if (buffer.hasRemaining()) this.message = new String(MySQLs.readBytes(buffer, array));
        }
    }

    public boolean isSuccess() {
        return this.statusCode == STATUSCODE_OK;
    }

    public String toMessageString(String defval) {
        if (message == null) return defval;
        return message;
    }

}
