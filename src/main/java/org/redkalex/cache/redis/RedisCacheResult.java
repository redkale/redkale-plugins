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

    //+   简单字符串类型 (不包含CRLF)
    //-   错误类型 (不包含CRLF)
    //':  整型
    //$   块字符串 
    //*   数组
    protected byte frameType;

    protected byte[] frameValue;  //(不包含CRLF)

    protected List<byte[]> frameList;  //(不包含CRLF)

    public RedisCacheResult prepare(byte byteType, byte[] val, List<byte[]> bytesList) {
        this.frameType = byteType;
        this.frameValue = val;
        this.frameList = bytesList;
        return this;
    }

    public Void getVoidValue() {
        return null;
    }

    public byte[] getFrameValue() {
        return frameValue;
    }

    public String getStringValue(String key, RedisCryptor cryptor) {
        if (frameValue == null) return null;
        String val = new String(frameValue, StandardCharsets.UTF_8);
        if (cryptor != null) val = cryptor.decrypt(key, val);
        return val;
    }

    public Double getDoubleValue(Double defvalue) {
        return frameValue == null ? defvalue : Double.parseDouble(new String(frameValue, StandardCharsets.UTF_8));
    }

    public Long getLongValue(Long defvalue) {
        return frameValue == null ? defvalue : Long.parseLong(new String(frameValue, StandardCharsets.UTF_8));
    }

    public Integer getIntValue(Integer defvalue) {
        return frameValue == null ? defvalue : Integer.parseInt(new String(frameValue, StandardCharsets.UTF_8));
    }

    public <T> T getObjectValue(String key, RedisCryptor cryptor, Type type) {
        return formatValue(key, cryptor, frameValue, type);
    }

    protected <T> Set<T> getSetValue(String key, RedisCryptor cryptor, Type type) {
        if (frameList == null || frameList.isEmpty()) return new LinkedHashSet<>();
        Set<T> set = new LinkedHashSet<>();
        for (byte[] bs : frameList) {
            set.add(formatValue(key, cryptor, bs, type));
        }
        return set;
    }

    protected <T> List<T> getListValue(String key, RedisCryptor cryptor, Type type) {
        if (frameList == null || frameList.isEmpty()) return new ArrayList<>();
        List<T> list = new ArrayList<>();
        for (byte[] bs : frameList) {
            list.add(formatValue(key, cryptor, bs, type));
        }
        return list;
    }

    protected <T> Map<String, T> getMapValue(String key, RedisCryptor cryptor, Type type) {
        if (frameList == null || frameList.isEmpty()) return new LinkedHashMap<>();
        Map<String, T> map = new LinkedHashMap<>();
        for (int i = 0; i < frameList.size(); i += 2) {
            byte[] bs1 = frameList.get(i);
            byte[] bs2 = frameList.get(i + 1);
            T val = formatValue(key, cryptor, bs2, type);
            if (val != null) map.put(formatValue(key, cryptor, bs1, String.class).toString(), val);
        }
        return map;
    }

    protected static <T> T formatValue(String key, RedisCryptor cryptor, byte[] frames, Type type) {
        if (frames == null) return null;
        if (type == byte[].class) return (T) frames;
        if (type == String.class) {
            String val = new String(frames, StandardCharsets.UTF_8);
            if (cryptor != null) val = cryptor.decrypt(key, val);
            return (T) val;
        }
        if (type == long.class || type == Long.class) {
            return (T) (Long) Long.parseLong(new String(frames, StandardCharsets.UTF_8));
        }
        if (type == double.class || type == Double.class) {
            return (T) (Double) Double.parseDouble(new String(frames, StandardCharsets.UTF_8));
        }
        if (cryptor != null) {
            String val = cryptor.decrypt(key, new String(frames, StandardCharsets.UTF_8));
            return (T) JsonConvert.root().convertFrom(type, val);
        }
        return (T) JsonConvert.root().convertFrom(type, frames);
    }

}
