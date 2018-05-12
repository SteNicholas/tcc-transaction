package org.mengyun.tcctransaction.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

/**
 * 可补偿事务方法注解
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Compensable {

    /**
     * 传播级别
     *
     * @return
     */
    public Propagation propagation() default Propagation.REQUIRED;

    /**
     * 确认方法
     *
     * @return
     */
    public String confirmMethod() default "";

    /**
     * 取消方法
     *
     * @return
     */
    public String cancelMethod() default "";

    /**
     * 事务上下文编辑器,用于设置和获取事务上下文
     *
     * @return
     */
    public Class<? extends TransactionContextEditor> transactionContextEditor() default DefaultTransactionContextEditor.class;

    /**
     * 是否同步确认
     *
     * @return
     */
    public boolean asyncConfirm() default false;

    /**
     * 是否同步取消
     *
     * @return
     */
    public boolean asyncCancel() default false;

    /**
     * 无事务上下文编辑器实现
     */
    class NullableTransactionContextEditor implements TransactionContextEditor {

        @Override
        public TransactionContext get(Object target, Method method, Object[] args) {
            return null;
        }

        @Override
        public void set(TransactionContext transactionContext, Object target, Method method, Object[] args) {

        }
    }

    /**
     * 默认事务上下文编辑器实现
     */
    class DefaultTransactionContextEditor implements TransactionContextEditor {

        @Override
        public TransactionContext get(Object target, Method method, Object[] args) {
            int position = getTransactionContextParamPosition(method.getParameterTypes());

            if (position >= 0) {
                return (TransactionContext) args[position];
            }
            return null;
        }

        @Override
        public void set(TransactionContext transactionContext, Object target, Method method, Object[] args) {
            int position = getTransactionContextParamPosition(method.getParameterTypes());

            if (position >= 0) {
                args[position] = transactionContext;
            }
        }

        /**
         * 根据参数类型数组获取事务上下文类型位置
         *
         * @param parameterTypes
         * @return
         */
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

        /**
         * 根据参数数组获取事务上下文
         *
         * @param args
         * @return
         */
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
}