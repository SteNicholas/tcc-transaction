package org.mengyun.tcctransaction;

import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionContextEditor;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;

import java.io.Serializable;

/**
 * 参与者
 */
public class Participant implements Serializable {

    private static final long serialVersionUID = 4127729421281425247L;

    /**
     * 事务编号,通过 TransactionXid.globalTransactionId属性关联参与者所属的事务
     */
    private TransactionXid xid;

    /**
     * 确认执行方法调用上下文
     */
    private InvocationContext confirmInvocationContext;

    /**
     * 取消执行方法调用上下文
     */
    private InvocationContext cancelInvocationContext;

    /**
     * 方法调用执行器
     */
    private Terminator terminator = new Terminator();

    /**
     * 事务上下文编辑器
     */
    Class<? extends TransactionContextEditor> transactionContextEditorClass;

    public Participant() {

    }

    /**
     * 参与者构造方法,通常在ResourceCoordinatorInterceptor切面中被调用
     *
     * @param xid
     * @param confirmInvocationContext
     * @param cancelInvocationContext
     * @param transactionContextEditorClass
     */
    public Participant(TransactionXid xid, InvocationContext confirmInvocationContext, InvocationContext cancelInvocationContext, Class<? extends TransactionContextEditor> transactionContextEditorClass) {
        this.xid = xid;
        this.confirmInvocationContext = confirmInvocationContext;
        this.cancelInvocationContext = cancelInvocationContext;
        this.transactionContextEditorClass = transactionContextEditorClass;
    }

    public Participant(InvocationContext confirmInvocationContext, InvocationContext cancelInvocationContext, Class<? extends TransactionContextEditor> transactionContextEditorClass) {
        this.confirmInvocationContext = confirmInvocationContext;
        this.cancelInvocationContext = cancelInvocationContext;
        this.transactionContextEditorClass = transactionContextEditorClass;
    }

    public void setXid(TransactionXid xid) {
        this.xid = xid;
    }

    /**
     * 回滚事务
     */
    public void rollback() {
        terminator.invoke(new TransactionContext(xid, TransactionStatus.CANCELLING.getId()), cancelInvocationContext, transactionContextEditorClass);
    }

    /**
     * 提交事务
     */
    public void commit() {
        terminator.invoke(new TransactionContext(xid, TransactionStatus.CONFIRMING.getId()), confirmInvocationContext, transactionContextEditorClass);
    }

    public Terminator getTerminator() {
        return terminator;
    }

    public TransactionXid getXid() {
        return xid;
    }

    public InvocationContext getConfirmInvocationContext() {
        return confirmInvocationContext;
    }

    public InvocationContext getCancelInvocationContext() {
        return cancelInvocationContext;
    }
}