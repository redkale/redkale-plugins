/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.protobuf;

import java.nio.ByteBuffer;
import org.redkale.convert.*;

/**
 *
 * @author zhangjx
 */
public class ProtobufByteBufferReader extends ProtobufReader {

    private ByteBuffer[] buffers;

    private int currentIndex = 0;

    private ByteBuffer currentBuffer;

    protected ConvertMask mask;

    protected ProtobufByteBufferReader(ConvertMask mask, ByteBuffer... buffers) {
        this.mask = mask;
        this.buffers = buffers;
        if (buffers != null && buffers.length > 0) this.currentBuffer = buffers[currentIndex];
    }

    @Override
    protected boolean recycle() {
        super.recycle();   // this.position 初始化值为-1
        this.currentIndex = 0;
        this.currentBuffer = null;
        this.buffers = null;
        this.mask = null;
        return false;
    }

    @Override
    protected byte currentByte() {
        return mask == null ? currentBuffer.get(currentBuffer.position()) : mask.unmask(currentBuffer.get(currentBuffer.position()));
    }

//------------------------------------------------------------
}
