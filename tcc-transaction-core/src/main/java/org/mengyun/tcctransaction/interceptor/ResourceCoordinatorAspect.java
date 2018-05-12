package org.mengyun.tcctransaction.interceptor;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

/**
 * 资源协调者切面,用于封装事务的参与者集合添加参与者
 * 通过org.aspectj.lang.annotation.@Pointcut + org.aspectj.lang.annotation.@Around注解,配置对@Compensable注解的方法进行拦截,
 * 调用ResourceCoordinatorInterceptor#interceptTransactionContextMethod(...)方法进行处理
 */
@Aspect
public abstract class ResourceCoordinatorAspect {

    private ResourceCoordinatorInterceptor resourceCoordinatorInterceptor;

    @Pointcut("@annotation(org.mengyun.tcctransaction.api.Compensable)")
    public void transactionContextCall() {

    }

    @Around("transactionContextCall()")
    public Object interceptTransactionContextMethod(ProceedingJoinPoint pjp) throws Throwable {
        return resourceCoordinatorInterceptor.interceptTransactionContextMethod(pjp);
    }

    public void setResourceCoordinatorInterceptor(ResourceCoordinatorInterceptor resourceCoordinatorInterceptor) {
        this.resourceCoordinatorInterceptor = resourceCoordinatorInterceptor;
    }

    public abstract int getOrder();
}