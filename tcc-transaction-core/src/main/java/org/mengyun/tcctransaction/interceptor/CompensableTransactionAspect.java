package org.mengyun.tcctransaction.interceptor;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

/**
 * 可补偿事务切面,用于封装事务逻辑(事务开启、传播、提交和回滚等)
 * 通过org.aspectj.lang.annotation.@Pointcut + org.aspectj.lang.annotation.@Around注解,配置对@Compensable注解的方法进行拦截,
 * 调用CompensableTransactionInterceptor#interceptCompensableMethod(...)方法进行处理
 */
@Aspect
public abstract class CompensableTransactionAspect {

    private CompensableTransactionInterceptor compensableTransactionInterceptor;

    public void setCompensableTransactionInterceptor(CompensableTransactionInterceptor compensableTransactionInterceptor) {
        this.compensableTransactionInterceptor = compensableTransactionInterceptor;
    }

    @Pointcut("@annotation(org.mengyun.tcctransaction.api.Compensable)")
    public void compensableService() {

    }

    @Around("compensableService()")
    public Object interceptCompensableMethod(ProceedingJoinPoint pjp) throws Throwable {
        return compensableTransactionInterceptor.interceptCompensableMethod(pjp);
    }

    public abstract int getOrder();
}