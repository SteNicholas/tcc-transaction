package org.mengyun.tcctransaction;


import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.common.TransactionType;

import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事务
 */
public class Transaction implements Serializable {

    private static final long serialVersionUID = 7291423944314337931L;

    /**
     * 事务编号,用于唯一标识一个事务,使用 UUID 算法生成保证唯一性
     */
    private TransactionXid xid;

    /**
     * 事务状态:TRYING, CONFIRMING, CANCELLING
     */
    private TransactionStatus status;

    /**
     * 事务类型,根事务:ROOT,分支事务:BRANCH
     */
    private TransactionType transactionType;

    /**
     * 重试次数
     */
    private volatile int retriedCount = 0;

    /**
     * 创建时间
     */
    private Date createTime = new Date();

    /**
     * 最后更新时间
     */
    private Date lastUpdateTime = new Date();

    /**
     * 版本号,用于乐观锁更新事务
     */
    private long version = 1;

    /**
     * 参与者集合,包括根事务和分支事务参与者
     */
    private List<Participant> participants = new ArrayList<Participant>();

    /**
     * 附带属性映射
     */
    private Map<String, Object> attachments = new ConcurrentHashMap<String, Object>();

    public Transaction() {

    }

    /**
     * 事务构造方法,参数为事务上下文,创建分支事务,通常在分支事务端被调用,
     * 用于根据从根事务端传递的事务上下文中创建分支事务.
     *
     * @param transactionContext
     */
    public Transaction(TransactionContext transactionContext) {
        //获取事务上下文事务编号
        this.xid = transactionContext.getXid();
        //事务状态默认为尝试中
        this.status = TransactionStatus.TRYING;
        //事务类型默认为分支事务
        this.transactionType = TransactionType.BRANCH;
    }

    /**
     * 事务构造方法,参数为事务类型,创建指定类型事务,通常会在根事务端被调用
     *
     * @param transactionType
     */
    public Transaction(TransactionType transactionType) {
        //获取创建新的事务编号
        this.xid = new TransactionXid();
        //事务状态默认为尝试中
        this.status = TransactionStatus.TRYING;
        //事务类型为参数指定类型
        this.transactionType = transactionType;
    }

    /**
     * 添加参与者
     *
     * @param participant
     */
    public void enlistParticipant(Participant participant) {
        participants.add(participant);
    }

    public Xid getXid() {
        return xid.clone();
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public List<Participant> getParticipants() {
        return participants;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void changeStatus(TransactionStatus status) {
        this.status = status;
    }

    /**
     * 提交事务:遍历所有事务参与者,调用参与者提交事务
     */
    public void commit() {
        for (Participant participant : participants) {
            participant.commit();
        }
    }

    /**
     * 回滚事务:遍历所有事务参与者,调用参与者回滚事务
     */
    public void rollback() {
        for (Participant participant : participants) {
            participant.rollback();
        }
    }

    public int getRetriedCount() {
        return retriedCount;
    }

    public void addRetriedCount() {
        this.retriedCount++;
    }

    public void resetRetriedCount(int retriedCount) {
        this.retriedCount = retriedCount;
    }

    public Map<String, Object> getAttachments() {
        return attachments;
    }

    public long getVersion() {
        return version;
    }

    public void updateVersion() {
        this.version++;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date date) {
        this.lastUpdateTime = date;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void updateTime() {
        this.lastUpdateTime = new Date();
    }
}