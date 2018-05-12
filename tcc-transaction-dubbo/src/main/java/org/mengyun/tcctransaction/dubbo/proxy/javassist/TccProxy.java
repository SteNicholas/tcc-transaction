/*
 * Copyright 1999-2011 Alibaba Group.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mengyun.tcctransaction.dubbo.proxy.javassist;

import com.alibaba.dubbo.common.utils.ClassHelper;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import org.mengyun.tcctransaction.api.Compensable;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TccProxy.
 *
 * @author qian.lei
 */
public abstract class TccProxy {
    /**
     * Proxy Class计数器,用于生成 Proxy 类名自增
     */
    private static final AtomicLong PROXY_CLASS_COUNTER = new AtomicLong(0);

    /**
     * Tcc Proxy包名
     */
    private static final String PACKAGE_NAME = TccProxy.class.getPackage().getName();

    public static final InvocationHandler RETURN_NULL_INVOKER = new InvocationHandler() {
        public Object invoke(Object proxy, Method method, Object[] args) {
            return null;
        }
    };

    public static final InvocationHandler THROW_UNSUPPORTED_INVOKER = new InvocationHandler() {
        public Object invoke(Object proxy, Method method, Object[] args) {
            throw new UnsupportedOperationException("Method [" + ReflectUtils.getName(method) + "] unimplemented.");
        }
    };

    /**
     * Proxy对象缓存
     * key:ClassLoader
     * value.key:Tcc Proxy标识,使用 Tcc Proxy实现接口名拼接
     * value.value:Tcc Proxy代理
     */
    private static final Map<ClassLoader, Map<String, Object>> ProxyCacheMap = new WeakHashMap<ClassLoader, Map<String, Object>>();

    /**
     * 等待生成Proxy Class生成标记
     */
    private static final Object PendingGenerationMarker = new Object();

    /**
     * Get proxy.
     *
     * @param ics interface class array.
     * @return TccProxy instance.
     */
    public static TccProxy getProxy(Class<?>... ics) {
        return getProxy(ClassHelper.getCallerClassLoader(TccProxy.class), ics);
    }

    /**
     * Get proxy.
     *
     * @param cl  class loader.
     * @param ics interface class array.
     * @return TccProxy instance.
     */
    public static TccProxy getProxy(ClassLoader cl, Class<?>... ics) {
        //判断接口类数组大小是否大于65535上限
        if (ics.length > 65535)
            throw new IllegalArgumentException("interface limit exceeded");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ics.length; i++) {
            String itf = ics[i].getName();
            //判断接口类是否为接口
            if (!ics[i].isInterface())
                throw new RuntimeException(itf + " is not a interface.");

            Class<?> tmp = null;
            try {
                //加载接口类
                tmp = Class.forName(itf, false, cl);
            } catch (ClassNotFoundException e) {
            }

            //判断加载类跟接口类是否相等
            if (tmp != ics[i])
                throw new IllegalArgumentException(ics[i] + " is not visible from class loader");

            sb.append(itf).append(';');
        }

        // use interface class name list as key.
        String key = sb.toString();

        // get cache by class loader.
        Map<String, Object> cache;
        synchronized (ProxyCacheMap) {
            cache = ProxyCacheMap.get(cl);
            if (cache == null) {
                cache = new HashMap<String, Object>();
                ProxyCacheMap.put(cl, cache);
            }
        }

        TccProxy proxy = null;
        synchronized (cache) {
            do {
                //从Proxy对象缓存获取Tcc Proxy
                Object value = cache.get(key);
                if (value instanceof Reference<?>) {
                    proxy = (TccProxy) ((Reference<?>) value).get();
                    if (proxy != null)
                        return proxy;
                }

                //若Proxy缓存中不存在,设置正在生成 Tcc Proxy代码标记,创建中时其他创建请求等待避免并发
                if (value == PendingGenerationMarker) {
                    try {
                        cache.wait();
                    } catch (InterruptedException e) {
                    }
                } else {
                    cache.put(key, PendingGenerationMarker);
                    break;
                }
            }
            while (true);
        }

        //Proxy Class计数器自增1获取计数
        long id = PROXY_CLASS_COUNTER.getAndIncrement();
        String pkg = null;
        TccClassGenerator ccp = null, ccm = null;
        try {
            //创建Tcc Class生成器实例
            ccp = TccClassGenerator.newInstance(cl);

            //方法签名集合
            Set<String> worked = new HashSet<String>();
            //方法集合
            List<Method> methods = new ArrayList<Method>();

            for (int i = 0; i < ics.length; i++) {
                //判断接口类修饰词是否为Public,否则使用接口包名
                if (!Modifier.isPublic(ics[i].getModifiers())) {
                    String npkg = ics[i].getPackage().getName();
                    if (pkg == null) {
                        pkg = npkg;
                    } else {
                        //判断是否实现两个非Public接口
                        if (!pkg.equals(npkg))
                            throw new IllegalArgumentException("non-public interfaces from different packages");
                    }
                }
                //添加生成类的接口
                ccp.addInterface(ics[i]);

                for (Method method : ics[i].getMethods()) {
                    String desc = ReflectUtils.getDesc(method);
                    if (worked.contains(desc))
                        continue;
                    //方法签名集合添加方法签名
                    worked.add(desc);
                    //生成接口方法
                    int ix = methods.size();
                    Class<?> rt = method.getReturnType();
                    Class<?>[] pts = method.getParameterTypes();

                    StringBuilder code = new StringBuilder("Object[] args = new Object[").append(pts.length).append("];");
                    for (int j = 0; j < pts.length; j++)
                        code.append(" args[").append(j).append("] = ($w)$").append(j + 1).append(";");
                    code.append(" Object ret = handler.invoke(this, methods[" + ix + "], args);");
                    if (!Void.TYPE.equals(rt))
                        code.append(" return ").append(asArgument(rt, "ret")).append(";");

                    //添加方法
                    methods.add(method);

                    StringBuilder compensableDesc = new StringBuilder();
                    Compensable compensable = method.getAnnotation(Compensable.class);
                    if (compensable != null) {
                        //添加生成类的方法
                        ccp.addMethod(true, method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(), code.toString());
                    } else {
                        ccp.addMethod(false, method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(), code.toString());
                    }
                }
            }

            if (pkg == null)
                pkg = PACKAGE_NAME;

            // create ProxyInstance class.
            String pcn = pkg + ".proxy" + id;
            //设置类名
            ccp.setClassName(pcn);
            //添加公共静态属性methods
            ccp.addField("public static java.lang.reflect.Method[] methods;");
            //添加私有属性handler
            ccp.addField("private " + InvocationHandler.class.getName() + " handler;");
            //添加构造器,参数为handler
            ccp.addConstructor(Modifier.PUBLIC, new Class<?>[]{InvocationHandler.class}, new Class<?>[0], "handler=$1;");
            //添加默认空构造器
            ccp.addDefaultConstructor();
            //生成ProxyInstance类
            Class<?> clazz = ccp.toClass();
            //设置静态属性methods
            clazz.getField("methods").set(null, methods.toArray(new Method[0]));

            // create TccProxy class.
            String fcn = TccProxy.class.getName() + id;
            //创建Tcc Class生成器实例
            ccm = TccClassGenerator.newInstance(cl);
            //设置类名
            ccm.setClassName(fcn);
            //添加默认空构造器
            ccm.addDefaultConstructor();
            //设置父类为TccProxy
            ccm.setSuperClass(TccProxy.class);
            //添加公共方法#newInstance(handler)
            ccm.addMethod("public Object newInstance(" + InvocationHandler.class.getName() + " h){ return new " + pcn + "($1); }");
            //生成TccProxy类
            Class<?> pc = ccm.toClass();
            //创建TccProxy实例
            proxy = (TccProxy) pc.newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            // release TccClassGenerator
            if (ccp != null)
                ccp.release();
            if (ccm != null)
                ccm.release();
            //唤醒Proxy缓存等待线程
            synchronized (cache) {
                if (proxy == null)
                    cache.remove(key);
                else
                    cache.put(key, new WeakReference<TccProxy>(proxy));
                cache.notifyAll();
            }
        }
        return proxy;
    }

    /**
     * get instance with default handler.
     *
     * @return instance.
     */
    public Object newInstance() {
        return newInstance(THROW_UNSUPPORTED_INVOKER);
    }

    /**
     * get instance with special handler.
     *
     * @return instance.
     */
    abstract public Object newInstance(InvocationHandler handler);

    protected TccProxy() {
    }

    /**
     * 生成返回
     *
     * @param cl
     * @param name
     * @return
     */
    private static String asArgument(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (Boolean.TYPE == cl)
                return name + "==null?false:((Boolean)" + name + ").booleanValue()";
            if (Byte.TYPE == cl)
                return name + "==null?(byte)0:((Byte)" + name + ").byteValue()";
            if (Character.TYPE == cl)
                return name + "==null?(char)0:((Character)" + name + ").charValue()";
            if (Double.TYPE == cl)
                return name + "==null?(double)0:((Double)" + name + ").doubleValue()";
            if (Float.TYPE == cl)
                return name + "==null?(float)0:((Float)" + name + ").floatValue()";
            if (Integer.TYPE == cl)
                return name + "==null?(int)0:((Integer)" + name + ").intValue()";
            if (Long.TYPE == cl)
                return name + "==null?(long)0:((Long)" + name + ").longValue()";
            if (Short.TYPE == cl)
                return name + "==null?(short)0:((Short)" + name + ").shortValue()";
            throw new RuntimeException(name + " is unknown primitive type.");
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }
}