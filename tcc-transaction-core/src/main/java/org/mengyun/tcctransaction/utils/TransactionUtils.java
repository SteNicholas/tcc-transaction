package org.mengyun.tcctransaction.utils;

import org.mengyun.tcctransaction.api.Propagation;
import org.mengyun.tcctransaction.api.TransactionContext;

/**
 * 事务工具类
 */
public class TransactionUtils {

    /**
     * 判断事务上下文是否合法,即在 Propagation.MANDATORY必须有在事务内
     * 不合法依据:传播级别是MANDATORY,事务未开启并且事务上下文为空
     *
     * @param isTransactionActive
     * @param propagation
     * @param transactionContext
     * @return
     */
    public static boolean isLegalTransactionContext(boolean isTransactionActive, Propagation propagation, TransactionContext transactionContext) {

        if (propagation.equals(Propagation.MANDATORY) && !isTransactionActive && transactionContext == null) {
            return false;
        }

        return true;
    }
}