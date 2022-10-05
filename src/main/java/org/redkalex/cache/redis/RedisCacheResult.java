/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.WorkThread;

/**
 *
 * @author zhangjx
 */
public class RedisCacheResult {

    protected final WorkThread thread = WorkThread.currWorkThread();

    protected RedisCacheRequest request;

    protected byte byteType;

    protected byte[] bytesValue;  //(不包含CRLF)

    protected List<byte[]> bytesList;  //(不包含CRLF)

    public RedisCacheResult prepare(byte byteType, byte[] val, List<byte[]> bytesList) {
        this.byteType = byteType;
        this.bytesValue = val;
        this.bytesList = bytesList;
        return this;
    }

    public Void getVoidValue() {
        return null;
    }

    public byte[] getBytesValue() {
        return bytesValue;
    }

    public String getStringValue(String key, RedisCryptor cryptor) {
        if (bytesValue == null) return null;
        String val = new String(bytesValue, StandardCharsets.UTF_8);
        if (cryptor != null) val = cryptor.decrypt(key, val);
        return val;
    }

    public Long getLongValue(Long defvalue) {
        return bytesValue == null ? defvalue : Long.parseLong(new String(bytesValue, StandardCharsets.UTF_8));
    }

    public Integer getIntValue(Integer defvalue) {
        return bytesValue == null ? defvalue : Integer.parseInt(new String(bytesValue, StandardCharsets.UTF_8));
    }

    public <T> T getObjectValue(String key, RedisCryptor cryptor, Type type) {
        return formatValue(key, cryptor, bytesValue, type);
    }

    protected <T> Collection<T> getCollectionValue(String key, RedisCryptor cryptor, boolean set, Type type) {
        if (bytesList == null || bytesList.isEmpty()) return set ? new LinkedHashSet<>() : new ArrayList<>();
        Collection<T> list = set ? new LinkedHashSet<>() : new ArrayList<>();
        for (byte[] bs : bytesList) {
            list.add(formatValue(key, cryptor, bs, type));
        }
        return list;
    }

    protected <T> Map<String, T> getMapValue(String key, RedisCryptor cryptor, Type type) {
        if (bytesList == null || bytesList.isEmpty()) return new LinkedHashMap<>();
        Map<String, T> map = new LinkedHashMap<>();
        for (int i = 0; i < bytesList.size(); i += 2) {
            byte[] bs1 = bytesList.get(i);
            byte[] bs2 = bytesList.get(i + 1);
            T val = formatValue(key, cryptor, bs2, type);
            if (val != null) map.put(formatValue(key, cryptor, bs1, String.class).toString(), val);
        }
        return map;
    }

    protected static <T> T formatValue(String key, RedisCryptor cryptor, byte[] bs, Type type) {
        if (bs == null) return null;
        if (type == byte[].class) return (T) bs;
        if (type == String.class) {
            String val = new String(bs, StandardCharsets.UTF_8);
            if (cryptor != null) val = cryptor.decrypt(key, val);
            return (T) val;
        }
        if (type == long.class) return (T) (Long) Long.parseLong(new String(bs, StandardCharsets.UTF_8));
        if (cryptor != null) {
            String val = cryptor.decrypt(key, new String(bs, StandardCharsets.UTF_8));
            return (T) JsonConvert.root().convertFrom(type, val);
        }
        return (T) JsonConvert.root().convertFrom(type, bs);
    }

}
