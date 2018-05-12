package org.mengyun.tcctransaction.dubbo.proxy.jdk;

import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.proxy.InvokerInvocationHandler;
import org.aspectj.lang.ProceedingJoinPoint;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.api.Propagation;
import org.mengyun.tcctransaction.dubbo.context.DubboTransactionContextEditor;
import org.mengyun.tcctransaction.interceptor.ResourceCoordinatorAspect;
import org.mengyun.tcctransaction.support.FactoryBuilder;
import org.mengyun.tcctransaction.utils.ReflectionUtils;

import java.lang.reflect.Method;

/**
 * Tcc调用处理器,用于调用Dubbo Service服务时使用ResourceCoordinatorInterceptor拦截处理
 */
public class TccInvokerInvocationHandler extends InvokerInvocationHandler {

    private Object target;

    public TccInvokerInvocationHandler(Invoker<?> handler) {
        super(handler);
    }

    public <T> TccInvokerInvocationHandler(T target, Invoker<T> invoker) {
        super(invoker);
        this.target = target;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Compensable compensable = method.getAnnotation(Compensable.class);
        if (compensable != null) {
            if (StringUtils.isEmpty(compensable.confirmMethod())) {
                //设置@Compensable注解属性
                ReflectionUtils.changeAnnotationValue(compensable, "confirmMethod", method.getName());
                ReflectionUtils.changeAnnotationValue(compensable, "cancelMethod", method.getName());
                ReflectionUtils.changeAnnotationValue(compensable, "transactionContextEditor", DubboTransactionContextEditor.class);
                ReflectionUtils.changeAnnotationValue(compensable, "propagation", Propagation.SUPPORTS);
            }

            //生成处理切面
            ProceedingJoinPoint pjp = new MethodProceedingJoinPoint(proxy, target, method, args);
            //获取资源协调者切面拦截处理,调用ResourceCoordinatorAspect#interceptTransactionContextMethod(...)方法对方法切面拦截处理,无需调用CompensableTransactionAspect切面因为传播级别为Propagation.SUPPORTS不会发起事务
            return FactoryBuilder.factoryOf(ResourceCoordinatorAspect.class).getInstance().interceptTransactionContextMethod(pjp);
        } else {
            return super.invoke(target, method, args);
        }
    }
}