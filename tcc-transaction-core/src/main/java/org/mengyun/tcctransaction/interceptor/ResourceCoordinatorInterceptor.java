package org.mengyun.tcctransaction.interceptor;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.mengyun.tcctransaction.InvocationContext;
import org.mengyun.tcctransaction.Participant;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionManager;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.support.FactoryBuilder;
import org.mengyun.tcctransaction.utils.CompensableMethodUtils;
import org.mengyun.tcctransaction.utils.ReflectionUtils;

import java.lang.reflect.Method;

/**
 * 资源协调者拦截器
 */
public class ResourceCoordinatorInterceptor {

    /**
     * 事务管理器
     */
    private TransactionManager transactionManager;

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    /**
     * 拦截事务上下文方法
     *
     * @param pjp
     * @return
     * @throws Throwable
     */
    public Object interceptTransactionContextMethod(ProceedingJoinPoint pjp) throws Throwable {
        //获取线程局部事务队列头部事务
        Transaction transaction = transactionManager.getCurrentTransaction();
        if (transaction != null) {
            switch (transaction.getStatus()) {
                case TRYING:
                    //添加事务参与者
                    enlistParticipant(pjp);
                    break;
                case CONFIRMING:
                    break;
                case CANCELLING:
                    break;
            }
        }

        //执行切面方法逻辑
        return pjp.proceed(pjp.getArgs());
    }

    /**
     * 添加参与者
     *
     * @param pjp
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    private void enlistParticipant(ProceedingJoinPoint pjp) throws IllegalAccessException, InstantiationException {
        //获取带@Compensable注解可补偿事务方法
        Method method = CompensableMethodUtils.getCompensableMethod(pjp);
        if (method == null) {
            throw new RuntimeException(String.format("join point not found method, point is : %s", pjp.getSignature().getName()));
        }
        Compensable compensable = method.getAnnotation(Compensable.class);
        //获取@Compensable注解确认方法
        String confirmMethodName = compensable.confirmMethod();
        //获取@Compensable注解取消方法
        String cancelMethodName = compensable.cancelMethod();
        //获取线程局部事务队列头部事务
        Transaction transaction = transactionManager.getCurrentTransaction();
        //创建分支事务编号
        TransactionXid xid = new TransactionXid(transaction.getXid().getGlobalTransactionId());

        if (FactoryBuilder.factoryOf(compensable.transactionContextEditor()).getInstance().get(pjp.getTarget(), method, pjp.getArgs()) == null) {
            FactoryBuilder.factoryOf(compensable.transactionContextEditor()).getInstance().set(new TransactionContext(xid, TransactionStatus.TRYING.getId()), pjp.getTarget(), ((MethodSignature) pjp.getSignature()).getMethod(), pjp.getArgs());
        }

        //反射获取切面声明目标类
        Class targetClass = ReflectionUtils.getDeclaringType(pjp.getTarget().getClass(), method.getName(), method.getParameterTypes());
        //获取确认方法调用上下文
        InvocationContext confirmInvocation = new InvocationContext(targetClass,
                confirmMethodName,
                method.getParameterTypes(), pjp.getArgs());
        //获取取消方法调用上下文
        InvocationContext cancelInvocation = new InvocationContext(targetClass,
                cancelMethodName,
                method.getParameterTypes(), pjp.getArgs());

        //创建事务参与者
        Participant participant =
                new Participant(
                        xid,
                        confirmInvocation,
                        cancelInvocation,
                        compensable.transactionContextEditor());

        //事务管理器添加参与者
        transactionManager.enlistParticipant(participant);
    }
}