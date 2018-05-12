package org.mengyun.tcctransaction.dubbo.context;

import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.fastjson.JSON;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionContextEditor;
import org.mengyun.tcctransaction.dubbo.constants.TransactionContextConstants;

import java.lang.reflect.Method;

/**
 * Dubbo事务上下文编辑器
 */
public class DubboTransactionContextEditor implements TransactionContextEditor {

    @Override
    public TransactionContext get(Object target, Method method, Object[] args) {
        String context = RpcContext.getContext().getAttachment(TransactionContextConstants.TRANSACTION_CONTEXT);

        if (StringUtils.isNotEmpty(context)) {
            return JSON.parseObject(context, TransactionContext.class);
        }

        return null;
    }

    @Override
    public void set(TransactionContext transactionContext, Object target, Method method, Object[] args) {
        //通过 Dubbo的隐式传参的方式避免在 Dubbo Service接口上声明TransactionContext参数对接口产生入侵
        RpcContext.getContext().setAttachment(TransactionContextConstants.TRANSACTION_CONTEXT, JSON.toJSONString(transactionContext));
    }
}