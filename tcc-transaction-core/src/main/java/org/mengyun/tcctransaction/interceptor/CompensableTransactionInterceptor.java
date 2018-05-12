package org.mengyun.tcctransaction.interceptor;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.mengyun.tcctransaction.NoExistedTransactionException;
import org.mengyun.tcctransaction.SystemException;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionManager;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.api.Propagation;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.MethodType;
import org.mengyun.tcctransaction.support.FactoryBuilder;
import org.mengyun.tcctransaction.utils.CompensableMethodUtils;
import org.mengyun.tcctransaction.utils.ReflectionUtils;
import org.mengyun.tcctransaction.utils.TransactionUtils;

import java.lang.reflect.Method;
import java.util.Set;

/**
 * 可补偿事务拦截器
 */
public class CompensableTransactionInterceptor {

    static final Logger logger = Logger.getLogger(CompensableTransactionInterceptor.class.getSimpleName());

    /**
     * 事务管理器
     */
    private TransactionManager transactionManager;

    /**
     * 延迟取消异常集合
     */
    private Set<Class<? extends Exception>> delayCancelExceptions;

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public void setDelayCancelExceptions(Set<Class<? extends Exception>> delayCancelExceptions) {
        this.delayCancelExceptions = delayCancelExceptions;
    }

    /**
     * 拦截可补偿事务方法
     *
     * @param pjp
     * @return
     * @throws Throwable
     */
    public Object interceptCompensableMethod(ProceedingJoinPoint pjp) throws Throwable {
        //获取带@Compensable注解可补偿事务方法
        Method method = CompensableMethodUtils.getCompensableMethod(pjp);

        Compensable compensable = method.getAnnotation(Compensable.class);
        Propagation propagation = compensable.propagation();
        //通过事务上下文编辑器单例从切面方法参数数组获取事务上下文
        TransactionContext transactionContext = FactoryBuilder.factoryOf(compensable.transactionContextEditor()).getInstance().get(pjp.getTarget(), method, pjp.getArgs());

        boolean asyncConfirm = compensable.asyncConfirm();

        boolean asyncCancel = compensable.asyncCancel();
        //获取当前线程是否在事务中即事务是否开启
        boolean isTransactionActive = transactionManager.isTransactionActive();
        //判断事务上下文是否合法,不合法则抛出异常
        if (!TransactionUtils.isLegalTransactionContext(isTransactionActive, propagation, transactionContext)) {
            throw new SystemException("no active compensable transaction while propagation is mandatory for method " + method.getName());
        }

        //根据传播级别、事务是否开启以及事务上下文计算方法类型
        MethodType methodType = CompensableMethodUtils.calculateMethodType(propagation, isTransactionActive, transactionContext);
        //按照方法类型进行事务方法处理:
        //(1)方法类型为 MethodType.ROOT时,发起根事务,判断条件如下二选一:1.事务传播级别为 Propagation.REQUIRED,并且当前没有事务;2.事务传播级别为 Propagation.REQUIRES_NEW,新建事务,如果当前存在事务,把当前事务挂起.此时,事务管理器的当前线程事务队列可能会存在多个事务;
        //(2)方法类型为 MethodType.PROVIDER时,发起分支事务,判断条件如下二选一:1.事务传播级别为 Propagation.REQUIRED,并且当前不存在事务,并且方法参数传递事务上下文;2.事务传播级别为 Propagation.MANDATORY,并且当前不存在事务,并且方法参数传递事务上下文;
        //其中,当前不存在事务,方法参数传递事务上下文表示当跨服务远程调用时,被调用服务本身( 服务提供者 )不在事务中,通过传递事务上下文参数,融入当前事务.
        //(3)方法类型为 MethodType.Normal时,不进行事务处理
        switch (methodType) {
            case ROOT:
                //ROOT类型方法处理:发起根事务
                return rootMethodProceed(pjp, asyncConfirm, asyncCancel);
            case PROVIDER:
                //PROVIDER类型方法处理:发起分支事务
                return providerMethodProceed(pjp, transactionContext, asyncConfirm, asyncCancel);
            default:
                //默认执行切面方法逻辑,不进行事务处理
                return pjp.proceed();
        }
    }

    /**
     * ROOT类型方法处理,发起 TCC整体流程
     *
     * @param pjp
     * @param asyncConfirm
     * @param asyncCancel
     * @return
     * @throws Throwable
     */
    private Object rootMethodProceed(ProceedingJoinPoint pjp, boolean asyncConfirm, boolean asyncCancel) throws Throwable {
        Object returnValue = null;

        Transaction transaction = null;
        try {
            //事务管理器开始事务,即发起根事务
            transaction = transactionManager.begin();
            try {
                //执行切面方法逻辑,即Try阶段逻辑
                returnValue = pjp.proceed();
            } catch (Throwable tryingException) {
                //执行方法逻辑引起异常判断是否为延迟取消异常,部分异常不适合立即回滚事务,是则同步事务,否则回滚事务
                if (isDelayCancelException(tryingException)) {
                    //同步事务
                    transactionManager.syncTransaction();
                } else {
                    logger.warn(String.format("compensable transaction trying failed. transaction content:%s", JSON.toJSONString(transaction)), tryingException);
                    //回滚事务
                    transactionManager.rollback(asyncCancel);
                }
                throw tryingException;
            }

            //执行方法逻辑无异常提交事务
            transactionManager.commit(asyncConfirm);
        } finally {
            //事务处理结束清理事务,将事务从当前线程事务队列移除,避免线程冲突
            transactionManager.cleanAfterCompletion(transaction);
        }

        return returnValue;
    }

    /**
     * PROVIDER类型方法处理,服务提供者参与 TCC整体流程
     *
     * @param pjp
     * @param transactionContext
     * @param asyncConfirm
     * @param asyncCancel
     * @return
     * @throws Throwable
     */
    private Object providerMethodProceed(ProceedingJoinPoint pjp, TransactionContext transactionContext, boolean asyncConfirm, boolean asyncCancel) throws Throwable {
        Transaction transaction = null;
        try {
            //当事务处于 TransactionStatus.TRYING时,调用 TransactionManager#propagationExistBegin(...) 方法,传播发起分支事务.发起分支事务完成后,调用 ProceedingJoinPoint#proceed()方法,执行切面方法逻辑(即 Try阶段逻辑);
            //当事务处于 TransactionStatus.CONFIRMING时,调用 TransactionManager#commit()方法,提交事务;
            //当事务处于 TransactionStatus.CANCELLING时,调用 TransactionManager#rollback()方法,提交事务.
            //传播发起分支事务是在根事务进行 Confirm/Cancel时,调用根事务的参与者集合提交或回滚事务时,进行远程服务方法调用的参与者,通过自身的事务编号关联传播的分支事务(两者的事务编号相等 ),进行事务的提交或回滚.
            //调用 TransactionManager#cleanAfterCompletion(...) 方法,将事务从当前线程事务队列移除,避免线程冲突.
            //当事务处于 TransactionStatus.CONFIRMING/TransactionStatus.CANCELLING时,调用ReflectionUtils#getNullValue(...)方法反射返回空值.Confirm/Cancel相关方法是通过AOP切面调用,只调用不处理返回值,但是不能没有返回值,直接返回空值.
            switch (TransactionStatus.valueOf(transactionContext.getStatus())) {
                case TRYING:
                    //传播发起分支事务
                    transaction = transactionManager.propagationNewBegin(transactionContext);
                    return pjp.proceed();
                case CONFIRMING:
                    try {
                        //传播获取分支事务
                        transaction = transactionManager.propagationExistBegin(transactionContext);
                        //提交事务
                        transactionManager.commit(asyncConfirm);
                    } catch (NoExistedTransactionException excepton) {
                        //the transaction has been commit,ignore it.
                    }
                    break;
                case CANCELLING:
                    try {
                        //传播获取分支事务
                        transaction = transactionManager.propagationExistBegin(transactionContext);
                        //回滚事务
                        transactionManager.rollback(asyncCancel);
                    } catch (NoExistedTransactionException exception) {
                        //the transaction has been rollback,ignore it.
                    }
                    break;
            }
        } finally {
            //事务处理结束清理事务,将事务从当前线程事务队列移除,避免线程冲突
            transactionManager.cleanAfterCompletion(transaction);
        }

        Method method = ((MethodSignature) (pjp.getSignature())).getMethod();
        //事务状态为TransactionStatus.CONFIRMING/TransactionStatus.CANCELLING反射返回空值
        return ReflectionUtils.getNullValue(method.getReturnType());
    }

    /**
     * 判断是否为延迟取消异常
     *
     * @param throwable
     * @return
     */
    private boolean isDelayCancelException(Throwable throwable) {
        if (delayCancelExceptions != null) {
            for (Class delayCancelException : delayCancelExceptions) {
                Throwable rootCause = ExceptionUtils.getRootCause(throwable);

                if (delayCancelException.isAssignableFrom(throwable.getClass())
                        || (rootCause != null && delayCancelException.isAssignableFrom(rootCause.getClass()))) {
                    return true;
                }
            }
        }

        return false;
    }
}