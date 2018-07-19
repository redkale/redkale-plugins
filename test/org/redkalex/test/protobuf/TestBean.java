/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.test.protobuf;

import org.redkale.convert.json.JsonConvert;
import org.redkale.util.Utility;
import org.redkalex.convert.pson.ProtobufConvert;

/**
 *
 * @author zhangjx
 */
public class TestBean {

    public short[] types = new short[]{1, 2, 3, 4, 5};

    public int id = 25;

    public String name = "redkale";

    public String email = "redkale@redkale.org";

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

    public static void main(String[] args) throws Throwable {
        TestBean bean = new TestBean();
        System.out.println(bean);
        byte[] bs = ProtobufConvert.root().convertTo(bean);
        Utility.println(null, bs);
        System.out.println(ProtobufConvert.root().convertFrom(TestBean.class, bs).toString());
    }
}
