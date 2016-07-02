/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.rest;

import java.lang.reflect.Method;
import java.util.*;
import jdk.internal.org.objectweb.asm.*;
import static jdk.internal.org.objectweb.asm.ClassWriter.COMPUTE_FRAMES;
import static jdk.internal.org.objectweb.asm.Opcodes.*;
import org.redkale.convert.bson.BsonConvert;
import org.redkale.net.http.*;
import org.redkale.net.sncp.*;
import org.redkale.service.*;
import org.redkale.util.*;
import static org.redkale.net.sncp.Sncp.hash;

/**
 *
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
final class ServletBuilder {

    private ServletBuilder() {
    }

    //待实现
    public static <T extends RestHttpServlet> T createRestServlet(final Class<T> baseServletClass, final String serviceName, final Class<? extends Service> serviceType) {
        if (baseServletClass == null || serviceType == null) return null;
        if (!RestHttpServlet.class.isAssignableFrom(baseServletClass)) return null;
        int mod = baseServletClass.getModifiers();
        if (!java.lang.reflect.Modifier.isPublic(mod)) return null;
        if (java.lang.reflect.Modifier.isAbstract(mod)) return null;
        final String supDynName = baseServletClass.getName().replace('.', '/');
        final String serviceDesc = Type.getDescriptor(serviceType);
        final String webServletDesc = Type.getDescriptor(WebServlet.class);
        final String convertDesc = Type.getDescriptor(BsonConvert.class);
        final String httpRequestDesc = Type.getDescriptor(HttpRequest.class);
        final String httpResponseDesc = Type.getDescriptor(HttpResponse.class);
        final String serviceTypeString = serviceType.getName().replace('.', '/');

        final RestController controller = serviceType.getAnnotation(RestController.class);
        if (controller != null && controller.ignore()) return null; //标记为ignore=true不创建Servlet
        ClassLoader loader = Sncp.class.getClassLoader();
        String newDynName = serviceTypeString.substring(0, serviceTypeString.lastIndexOf('/') + 1) + "_Dyn" + serviceType.getSimpleName() + RestHttpServlet.class.getSimpleName();
        if (!serviceName.isEmpty()) {
            boolean normal = true;
            for (char ch : serviceName.toCharArray()) {//含特殊字符的使用hash值
                if (!((ch >= '0' && ch <= '9') || ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'))) normal = false;
            }
            newDynName += "_" + (normal ? serviceName : hash(serviceName));
        }
        try {
            return ((Class<T>) Class.forName(newDynName.replace('/', '.'))).newInstance();
        } catch (Exception ex) {
        }
        //------------------------------------------------------------------------------
        final String defmodulename = (controller != null && !controller.value().isEmpty()) ? controller.value() : serviceType.getSimpleName().replace("Service", "");
        for (char ch : defmodulename.toCharArray()) {
            if (!((ch >= '0' && ch <= '9') || ch == '$' || ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'))) { //不能含特殊字符
                throw new RuntimeException(serviceType.getName() + " has illeal " + RestController.class.getSimpleName() + ".value, only 0-9 a-z A-Z _ $");
            }
        }

        ClassWriter cw = new ClassWriter(COMPUTE_FRAMES);
        FieldVisitor fv;
        AsmMethodVisitor mv;
        AnnotationVisitor av0;

        cw.visit(V1_8, ACC_PUBLIC + ACC_FINAL + ACC_SUPER, newDynName, null, supDynName, null);
        { //注入 @WebServlet 注解
            av0 = cw.visitAnnotation(webServletDesc, true);
            {
                AnnotationVisitor av1 = av0.visitArray("value");
                av1.visit(null, "/" + defmodulename.toLowerCase() + "/*");
                av1.visitEnd();
            }
            av0.visit("moduleid", controller == null ? 0 : controller.module());
            av0.visit("repair", controller == null ? true : controller.repair());
            av0.visitEnd();
        }

        {  //注入 @Resource  private XXXService _service;
            fv = cw.visitField(ACC_PRIVATE, "_service", serviceDesc, null, null);
            av0 = fv.visitAnnotation("Ljavax/annotation/Resource;", true);
            av0.visitEnd();
            fv.visitEnd();
        }
        { //构造函数
            mv = new AsmMethodVisitor(cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null));
            //mv.setDebug(true);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitMethodInsn(INVOKESPECIAL, supDynName, "<init>", "()V", false);
            mv.visitInsn(RETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();
        }

        final List<MappingEntry> entrys = new ArrayList<>();
        for (final Method method : serviceType.getMethods()) {
            RestMapping[] mappings = method.getAnnotationsByType(RestMapping.class);
            boolean ignore = false;
            for (RestMapping mapping : mappings) {
                if (mapping.ignore()) {
                    ignore = true;
                    break;
                }
            }
            if (ignore) continue;
            if (mappings.length == 0) { //没有RestMapping，设置一个默认值
                MappingEntry entry = new MappingEntry(null, defmodulename, method);
                if (entrys.contains(entry)) throw new RuntimeException(serviceType.getName() + " on " + method.getName() + " 's mapping(" + entry.name + ") is repeat");
                entrys.add(entry);
            } else {
                for (RestMapping mapping : mappings) {
                    MappingEntry entry = new MappingEntry(mapping, defmodulename, method);
                    if (entrys.contains(entry)) throw new RuntimeException(serviceType.getName() + " on " + method.getName() + " 's mapping(" + entry.name + ") is repeat");
                    entrys.add(entry);
                }
            }
        }
        int i = - 1;
        for (final MappingEntry entry : entrys) {
            final Method method = entry.mappingMethod;
            final Class returnType = method.getReturnType();
            final String methodDesc = Type.getMethodDescriptor(method);
            final Class[] paramtypes = method.getParameterTypes();
            final int index = ++i;

            mv = new AsmMethodVisitor(cw.visitMethod(ACC_PUBLIC, entry.name, "(" + httpRequestDesc + httpResponseDesc + ")V", null, new String[]{"java/io/IOException"}));
            //mv.setDebug(true);
            if (entry.authignore) {
                av0 = mv.visitAnnotation("Lorg/redkale/net/http/BasedHttpServlet$AuthIgnore;", true);
                av0.visitEnd();
            }
            {
                av0 = mv.visitAnnotation("Lorg/redkale/net/http/BasedHttpServlet$WebAction;", true);
                av0.visit("url", "/" + defmodulename.toLowerCase() + "/" + entry.name);
                av0.visit("actionid", entry.actionid);
                av0.visitEnd();
            }
        }
        cw.visitEnd();
        byte[] bytes = cw.toByteArray();
        Class<?> newClazz = new ClassLoader(loader) {
            public final Class<?> loadClass(String name, byte[] b) {
                return defineClass(name, b, 0, b.length);
            }
        }.loadClass(newDynName.replace('/', '.'), bytes);
        try {
            return ((Class<T>) newClazz).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class MappingEntry {

        private static final RestMapping DEFAULT__MAPPING;

        static {
            try {
                DEFAULT__MAPPING = MappingEntry.class.getDeclaredMethod("mapping").getAnnotation(RestMapping.class);
            } catch (Exception e) {
                throw new Error(e);
            }
        }

        public MappingEntry(RestMapping mapping, final String defmodulename, Method method) {
            if (mapping == null) mapping = DEFAULT__MAPPING;
            this.ignore = mapping.ignore();
            String n = mapping.name().toLowerCase();
            if (n.isEmpty()) n = method.getName().toLowerCase().replace(defmodulename.toLowerCase(), "");
            this.name = n;
            this.mappingMethod = method;
            this.method = mapping.method();
            this.authignore = mapping.authignore();
            this.actionid = mapping.actionid();
            this.contentType = mapping.contentType();
            this.jsvar = mapping.jsvar();
        }

        public final Method mappingMethod;

        public final boolean ignore;

        public final String name;

        public final String[] method;

        public final boolean authignore;

        public final int actionid;

        public final String contentType;

        public final String jsvar;

        @RestMapping()
        private void mapping() { //用于获取RestMapping 默认值
        }

        @Override
        public int hashCode() {
            return this.name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            return this.name.equals(((MappingEntry) obj).name);
        }

    }
}
