/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.lang.reflect.Type;
import java.math.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.client.ClientResult;
import org.redkale.source.CacheScoredValue;

/**
 *
 * @author zhangjx
 */
public class RedisCacheResult implements ClientResult {

    //$   块字符串类型
    //*   数组
    //+   简单字符串类型
    //-   错误类型
    //:   整型
    protected byte frameType;

    protected byte[] frameCursor;

    protected byte[] frameValue;

    protected List<byte[]> frameList;

    public RedisCacheResult prepare(byte byteType, byte[] frameCursor, byte[] frameValue, List<byte[]> frameList) {
        this.frameType = byteType;
        this.frameCursor = frameCursor;
        this.frameValue = frameValue;
        this.frameList = frameList;
        return this;
    }

    @Override
    public boolean isKeepAlive() {
        return true;
    }

    public Void getVoidValue() {
        return null;
    }

    public byte[] getFrameValue() {
        return frameValue;
    }

    public int getCursor() {
        if (frameCursor == null || frameCursor.length < 1) {
            return -1;
        } else {
            return Integer.parseInt(new String(frameCursor));
        }
    }

    public Boolean getBoolValue() {
        if (frameValue == null) {
            return false;
        }
        String val = new String(frameValue, StandardCharsets.UTF_8);
        if ("OK".equals(val)) {
            return true;
        }
        if (val.isEmpty()) {
            return false;
        }
        for (char ch : val.toCharArray()) {
            if (!Character.isDigit(ch)) {
                return false;
            }
        }
        return Integer.parseInt(val) > 0;
    }

    public Double getDoubleValue(Double defValue) {
        if (frameValue == null) {
            return defValue;
        }
        String val = new String(frameValue, StandardCharsets.UTF_8);
        if ("nan".equalsIgnoreCase(val) || "-nan".equalsIgnoreCase(val)) {
            return Double.NaN;
        } else if ("inf".equalsIgnoreCase(val)) {
            return Double.POSITIVE_INFINITY;
        } else if ("-inf".equalsIgnoreCase(val)) {
            return Double.NEGATIVE_INFINITY;
        } else if ("-1".equalsIgnoreCase(val)) {
            return -1.0;
        } else if ("0".equalsIgnoreCase(val)) {
            return 0.0;
        } else if ("1".equalsIgnoreCase(val)) {
            return 1.0;
        } else {
            return Double.parseDouble(val);
        }
    }

    public Long getLongValue(Long defValue) {
        if (frameValue == null) {
            return defValue;
        }
        String val = new String(frameValue, StandardCharsets.UTF_8);
        if ("-1".equalsIgnoreCase(val)) {
            return -1L;
        } else if ("0".equalsIgnoreCase(val)) {
            return 0L;
        } else if ("1".equalsIgnoreCase(val)) {
            return 1L;
        } else {
            return Long.parseLong(val);
        }
    }

    public Integer getIntValue(Integer defValue) {
        if (frameValue == null) {
            return defValue;
        }
        String val = new String(frameValue, StandardCharsets.UTF_8);
        if ("-1".equalsIgnoreCase(val)) {
            return -1;
        } else if ("0".equalsIgnoreCase(val)) {
            return 0;
        } else if ("1".equalsIgnoreCase(val)) {
            return 1;
        } else {
            return Integer.parseInt(val);
        }
    }

    public <T> T getObjectValue(String key, RedisCryptor cryptor, Type type) {
        return decodeValue(key, cryptor, frameValue, type);
    }

    protected <T> Set<T> getSetValue(String key, RedisCryptor cryptor, Type type) {
        if (frameList == null || frameList.isEmpty()) {
            return new LinkedHashSet<>();
        }
        Set<T> set = new LinkedHashSet<>();
        for (byte[] bs : frameList) {
            set.add(decodeValue(key, cryptor, bs, type));
        }
        return set;
    }

    protected List<CacheScoredValue> getScoreListValue(String key, RedisCryptor cryptor, Type scoreType) {
        if (frameList == null || frameList.isEmpty()) {
            return new ArrayList<>();
        }
        List<CacheScoredValue> set = new ArrayList<>();
        for (int i = 0; i < frameList.size(); i += 2) {
            byte[] bs1 = frameList.get(i);
            byte[] bs2 = frameList.get(i + 1);
            Number val = decodeValue(key, cryptor, bs2, scoreType);
            if (val != null) {
                set.add(CacheScoredValue.create(val, new String(bs1, StandardCharsets.UTF_8)));
            }
        }
        return set;
    }

    protected <T> List<T> getListValue(String key, RedisCryptor cryptor, Type type) {
        if (frameList == null || frameList.isEmpty()) {
            return new ArrayList<>();
        }
        List<T> list = new ArrayList<>();
        for (byte[] bs : frameList) {
            list.add(decodeValue(key, cryptor, bs, type));
        }
        return list;
    }

    protected <T> Map<String, T> getMapValue(String key, RedisCryptor cryptor, Type type) {
        if (frameList == null || frameList.isEmpty()) {
            return new LinkedHashMap<>();
        }
        Map<String, T> map = new LinkedHashMap<>();
        for (int i = 0; i < frameList.size(); i += 2) {
            byte[] bs1 = frameList.get(i);
            byte[] bs2 = frameList.get(i + 1);
            T val = decodeValue(key, cryptor, bs2, type);
            if (val != null) {
                map.put(decodeValue(key, cryptor, bs1, String.class).toString(), val);
            }
        }
        return map;
    }

    protected static <T> T decodeValue(String key, RedisCryptor cryptor, byte[] frames, Type type) {
        if (frames == null) {
            return null;
        }
        if (type == byte[].class) {
            return (T) frames;
        }
        if (type == String.class) {
            String val = new String(frames, StandardCharsets.UTF_8);
            if (cryptor != null) {
                val = cryptor.decrypt(key, val);
            }
            return (T) val;
        }
        if (type == int.class || type == Integer.class) {
            return (T) (Integer) Integer.parseInt(new String(frames, StandardCharsets.UTF_8));
        }
        if (type == long.class || type == Long.class) {
            return (T) (Long) Long.parseLong(new String(frames, StandardCharsets.UTF_8));
        }
        if (type == float.class || type == Float.class) {
            return (T) (Float) Float.parseFloat(new String(frames, StandardCharsets.UTF_8));
        }
        if (type == BigInteger.class) {
            return (T) new BigInteger(new String(frames, StandardCharsets.UTF_8));
        }
        if (type == BigDecimal.class) {
            return (T) new BigDecimal(new String(frames, StandardCharsets.UTF_8));
        }
        if (type == boolean.class || type == Boolean.class) {
            String v = new String(frames, StandardCharsets.UTF_8);
            return (T) (Boolean) ("t".equalsIgnoreCase(v) || "1".equals(v));
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{type: ").append(frameType);
        if (frameValue != null) {
            sb.append(", value: ").append(new String(frameValue, StandardCharsets.UTF_8));
        }
        if (frameList != null) {
            sb.append(", list: [");
            boolean first = true;
            for (byte[] bs : frameList) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(bs == null ? null : new String(bs, StandardCharsets.UTF_8));
                first = false;
            }
            sb.append("]");
        }
        return sb.append("}").toString();
    }

}
