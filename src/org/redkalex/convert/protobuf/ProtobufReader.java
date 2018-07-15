/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.protobuf;

import org.redkale.convert.*;
import org.redkale.util.ObjectPool;

/**
 *
 * @author zhangjx
 */
public class ProtobufReader extends Reader {

    protected int position = -1;

    private byte[] content;

    public static ObjectPool<ProtobufReader> createPool(int max) {
        return new ObjectPool<>(max, (Object... params) -> new ProtobufReader(), null, (t) -> t.recycle());
    }

    public ProtobufReader() {
    }

    public ProtobufReader(byte[] bytes) {
        setBytes(bytes, 0, bytes.length);
    }

    public ProtobufReader(byte[] bytes, int start, int len) {
        setBytes(bytes, start, len);
    }

    public final void setBytes(byte[] bytes) {
        if (bytes == null) {
            this.position = 0;
        } else {
            setBytes(bytes, 0, bytes.length);
        }
    }

    public final void setBytes(byte[] bytes, int start, int len) {
        if (bytes == null) {
            this.position = 0;
        } else {
            this.content = bytes;
            this.position = start - 1;
        }
    }

    protected boolean recycle() {
        this.position = -1;
        this.content = null;
        return true;
    }

    public void close() {
        this.recycle();
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void skipValue() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readBlank() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readObjectE(Class clazz) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int readArrayB() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readArrayE() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int readMapB() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readMapE() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DeMember readFieldName(DeMember[] members) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean readBoolean() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public byte readByte() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public char readChar() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public short readShort() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int readInt() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long readLong() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public float readFloat() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public double readDouble() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String readSmallString() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String readClassName() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String readString() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
