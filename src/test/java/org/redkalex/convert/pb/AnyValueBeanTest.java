/*
 */

package org.redkalex.convert.pb;

import org.junit.jupiter.api.Test;
import org.redkale.util.AnyValueWriter;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class AnyValueBeanTest {

    public static void main(String[] args) throws Throwable {
        AnyValueBeanTest test = new AnyValueBeanTest();
        test.run1();
    }

    // protoc --java_out=D:\Java-Projects\RedkalePluginProject\src\test\java
    // --proto_path=D:\Java-Projects\RedkalePluginProject\src\test\java\org\redkalex\convert\pb AnyValueBean.proto
    @Test
    public void run1() throws Exception {
        AnyValueWriter bean = createAnyValueWriter();
        AnyValueBeanOuterClass.AnyValueBean.Builder builder = AnyValueBeanOuterClass.AnyValueBean.newBuilder();
        AnyValueBeanOuterClass.AnyValueBean bean2 = createAnyValueBean(bean, builder);
        byte[] bs2 = bean2.toByteArray();
        Utility.println("proto-buf ", bs2);
    }

    private static AnyValueWriter createAnyValueWriter() {
        AnyValueWriter writer = AnyValueWriter.create();
        writer.addValue("name", "aaa");
        writer.addValue("name", "bbb");
        writer.addValue("node", AnyValueWriter.create("id", "111"));
        writer.addValue("node", AnyValueWriter.create("id", "222"));
        writer.addValue("node", AnyValueWriter.create("id", "333"));
        return writer;
    }

    private static AnyValueBeanOuterClass.AnyValueBean createAnyValueBean(
            AnyValueWriter bean, AnyValueBeanOuterClass.AnyValueBean.Builder builder) {
        bean.forEach(
                (k, v) -> {
                    AnyValueBeanOuterClass.AnyValueBean.StringEntry.Builder b =
                            AnyValueBeanOuterClass.AnyValueBean.StringEntry.newBuilder();
                    b.setName(k);
                    b.setValue(v);
                    builder.addStringEntrys(b.build());
                },
                (k, v) -> {
                    AnyValueBeanOuterClass.AnyValueBean.AnyValueEntry.Builder b =
                            AnyValueBeanOuterClass.AnyValueBean.AnyValueEntry.newBuilder();
                    b.setName(k);
                    AnyValueBeanOuterClass.AnyValueBean.Builder vv = AnyValueBeanOuterClass.AnyValueBean.newBuilder();
                    b.setValue(createAnyValueBean((AnyValueWriter) v, vv));
                    builder.addAnyEntrys(b.build());
                });
        AnyValueBeanOuterClass.AnyValueBean bean2 = builder.build();
        return bean2;
    }
}
