/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.pson;

import java.lang.reflect.Method;
import java.util.*;
import org.redkale.convert.*;

/**
 * 枚举 的SimpledCoder实现
 *
 * <p>
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 * @param <R> Reader输入的子类型
 * @param <W> Writer输出的子类型
 * @param <E> Enum的子类
 */
public class ProtobufEnumSimpledCoder<R extends Reader, W extends Writer, E extends Enum> extends SimpledCoder<R, W, E> {

    private final Class<E> type;

    private final Map<E, Integer> values1 = new HashMap<>();

    private final Map<Integer, E> values2 = new HashMap<>();

    public ProtobufEnumSimpledCoder(Class<E> type) {
        this.type = type;
        try {
            final Method method = type.getMethod("values");
            int index = -1;
            for (E item : (E[]) method.invoke(null)) {
                values1.put(item, ++index);
                values2.put(index, item);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void convertTo(final W out, final E value) {
        if (value == null) {
            out.writeNull();
        } else {
            ((ProtobufWriter)out).writeUInt32(values1.getOrDefault(value, -1));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public E convertFrom(final R in) {
        int value = ((ProtobufReader)in).readRawVarint32();
        if (value == -1) return null;
        return values2.get(value);
    }

    @Override
    public Class<E> getType() {
        return type;
    }

}
