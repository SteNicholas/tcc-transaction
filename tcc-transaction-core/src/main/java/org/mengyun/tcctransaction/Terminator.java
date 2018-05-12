package org.mengyun.tcctransaction;

import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionContextEditor;
import org.mengyun.tcctransaction.support.FactoryBuilder;
import org.mengyun.tcctransaction.utils.StringUtils;

import java.io.Serializable;
import java.lang.reflect.Method;

/**
 * 调用执行器
 */
public class Terminator implements Serializable {

    private static final long serialVersionUID = -164958655471605778L;

    public Terminator() {

    }

    /**
     * 执行方法调用:
     * (1)根据调用上下文的目标类获取目标类单例;
     * (2)根据调用上下文的方法名、参数类型数组反射获取调用方法;
     * (3)根据调用上下文的参数数组执行目标类实例方法调用
     *
     * @param transactionContext
     * @param invocationContext
     * @param transactionContextEditorClass
     * @return
     */
    public Object invoke(TransactionContext transactionContext, InvocationContext invocationContext, Class<? extends TransactionContextEditor> transactionContextEditorClass) {
        if (StringUtils.isNotEmpty(invocationContext.getMethodName())) {
            try {
                //通过工厂构造器根据调用上下文的目标类获取目标类单例
                Object target = FactoryBuilder.factoryOf(invocationContext.getTargetClass()).getInstance();

                Method method = null;

                //根据调用上下文的方法名、参数类型数组反射获取调用方法
                method = target.getClass().getMethod(invocationContext.getMethodName(), invocationContext.getParameterTypes());

                //获取事务上下文编辑器实例设置事务上下文到方法参数数组
                FactoryBuilder.factoryOf(transactionContextEditorClass).getInstance().set(transactionContext, target, method, invocationContext.getArgs());

                //根据调用上下文的参数数组执行目标类实例方法调用
                return method.invoke(target, invocationContext.getArgs());
            } catch (Exception e) {
                throw new SystemException(e);
            }
        }
        return null;
    }
}