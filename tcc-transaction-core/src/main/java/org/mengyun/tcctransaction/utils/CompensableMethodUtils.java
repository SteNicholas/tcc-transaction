package org.mengyun.tcctransaction.utils;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.api.Propagation;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.common.MethodType;

import java.lang.reflect.Method;

/**
 * 可补偿事务方法工具类
 */
public class CompensableMethodUtils {

    /**
     * 获取带@Compensable注解可补偿事务方法
     *
     * @param pjp
     * @return
     */
    public static Method getCompensableMethod(ProceedingJoinPoint pjp) {
        //获取切面代理方法
        Method method = ((MethodSignature) (pjp.getSignature())).getMethod();

        if (method.getAnnotation(Compensable.class) == null) {
            try {
                //获取切面可补偿事务方法
                method = pjp.getTarget().getClass().getMethod(method.getName(), method.getParameterTypes());
            } catch (NoSuchMethodException e) {
                return null;
            }
        }
        return method;
    }

    /**
     * 计算方法类型
     *
     * @param propagation
     * @param isTransactionActive
     * @param transactionContext
     * @return
     */
    public static MethodType calculateMethodType(Propagation propagation, boolean isTransactionActive, TransactionContext transactionContext) {
        if ((propagation.equals(Propagation.REQUIRED) && !isTransactionActive && transactionContext == null) ||
                propagation.equals(Propagation.REQUIRES_NEW)) {
            //传播级别为REQUIRED,事务未开启并且事务上下文为空或者传播级别为REQUIRES_NEW,方法类型为ROOT
            return MethodType.ROOT;
        } else if ((propagation.equals(Propagation.REQUIRED) || propagation.equals(Propagation.MANDATORY)) && !isTransactionActive && transactionContext != null) {
            //传播级别为REQUIRED或者MANDATORY,事务未开启并且事务上下文不为空,方法类型为PROVIDER
            return MethodType.PROVIDER;
        } else {
            return MethodType.NORMAL;
        }
    }

    public static MethodType calculateMethodType(TransactionContext transactionContext, boolean isCompensable) {
        if (transactionContext == null && isCompensable) {
            //isRootTransactionMethod
            return MethodType.ROOT;
        } else if (transactionContext == null && !isCompensable) {
            //isSoaConsumer
            return MethodType.CONSUMER;
        } else if (transactionContext != null && isCompensable) {
            //isSoaProvider
            return MethodType.PROVIDER;
        } else {
            return MethodType.NORMAL;
        }
    }

    public static int getTransactionContextParamPosition(Class<?>[] parameterTypes) {
        int position = -1;

        for (int i = 0; i < parameterTypes.length; i++) {
            if (parameterTypes[i].equals(org.mengyun.tcctransaction.api.TransactionContext.class)) {
                position = i;
                break;
            }
        }
        return position;
    }

    public static TransactionContext getTransactionContextFromArgs(Object[] args) {

        TransactionContext transactionContext = null;

        for (Object arg : args) {
            if (arg != null && org.mengyun.tcctransaction.api.TransactionContext.class.isAssignableFrom(arg.getClass())) {

                transactionContext = (org.mengyun.tcctransaction.api.TransactionContext) arg;
            }
        }

        return transactionContext;
    }
}