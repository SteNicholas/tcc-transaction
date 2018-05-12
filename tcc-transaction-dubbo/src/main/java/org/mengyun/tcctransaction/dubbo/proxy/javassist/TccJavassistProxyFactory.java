package org.mengyun.tcctransaction.dubbo.proxy.javassist;

import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.proxy.InvokerInvocationHandler;
import com.alibaba.dubbo.rpc.proxy.javassist.JavassistProxyFactory;

/**
 * Tcc Javassist代理工厂
 */
public class TccJavassistProxyFactory extends JavassistProxyFactory {

    @SuppressWarnings("unchecked")
    public <T> T getProxy(Invoker<T> invoker, Class<?>[] interfaces) {
        return (T) TccProxy.getProxy(interfaces).newInstance(new InvokerInvocationHandler(invoker));
    }
}