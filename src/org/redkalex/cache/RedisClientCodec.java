/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import org.redkale.net.client.*;
import org.redkale.util.ByteArray;

/**
 *
 * @author zhangjx
 */
public class RedisClientCodec extends ClientCodec<RedisClientRequest, Serializable> {

    protected static final byte DOLLAR_BYTE = '$'; //对于批量字符串，回复的第一个字节是“$”

    protected static final byte ASTERISK_BYTE = '*'; //对于数组，回复的第一个字节是“*”

    protected static final byte PLUS_BYTE = '+'; //对于简单字符串，回复的第一个字节是“+”

    protected static final byte MINUS_BYTE = '-';  //对于错误，回复的第一个字节是“ - ”

    protected static final byte COLON_BYTE = ':';  //对于整数，回复的第一个字节是“:”

    protected byte halfFrameCmd;

    protected ByteArray halfFrameBytes;

    @Override //解析完成返回true，还需要继续读取返回false; 返回true后array会clear
    public boolean codecResult(ClientConnection conn, List<RedisClientRequest> requests, final ByteBuffer realbuf, ByteArray array) {
        if (!realbuf.hasRemaining()) return false;
        array.clear();
        ByteBuffer buffer = realbuf;
        //System.out.println("realbuf读到的长度: " + realbuf.remaining() + ", halfFrameBytes=" + (halfFrameBytes == null ? -1 : halfFrameBytes.length()));
//        if (halfFrameBytes != null) {  //存在半包
//            int remain = realbuf.remaining();
//            if (halfFrameCmd == 0) {
//                int cha = 5 - halfFrameBytes.length();
//                if (remain < cha) { //还是不够5字节
//                    halfFrameBytes.put(realbuf);
//                    return false;
//                }
//                halfFrameBytes.put(realbuf, cha);
//                halfFrameCmd = (char) halfFrameBytes.get(0);
//                halfFrameLength = halfFrameBytes.getInt(1);
//            }
//            //此时必然有halfFrameCmd、halfFrameLength
//            int bulen = halfFrameLength + 1 - halfFrameBytes.length(); //还差多少字节
//            if (bulen > remain) { //不够，继续读取
//                halfFrameBytes.put(realbuf);
//                return false;
//            }
//            halfFrameBytes.put(realbuf, bulen);
//            //此时halfFrameBytes是完整的frame数据            
//            buffer = ByteBuffer.wrap(halfFrameBytes.content(), 0, halfFrameBytes.length());
//            halfFrameBytes = null;
//            halfFrameCmd = 0;
//            halfFrameLength = 0;
//        } else if (realbuf.remaining() < 3) { //第一次就只有几个字节buffer
//            halfFrameBytes = new ByteArray();
//            halfFrameBytes.clear();
//            halfFrameBytes.put(realbuf);
//            return false;
//        }
        //buffer必然包含一个完整的frame数据
        return true;
    }

}
