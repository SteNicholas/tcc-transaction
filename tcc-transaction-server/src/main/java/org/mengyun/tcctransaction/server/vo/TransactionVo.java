package org.mengyun.tcctransaction.server.vo;

import java.util.Date;

/**
 * 事务Vo
 */
public class TransactionVo {

    /**
     * 领域
     */
    private String domain;

    /**
     * 全局事务编号
     */
    private String globalTxId;

    /**
     * 分支事务编号
     */
    private String branchQualifier;

    /**
     * 事务状态
     */
    private Integer status;

    /**
     * 事务类型
     */
    private Integer transactionType;

    /**
     * 重试次数
     */
    private Integer retriedCount;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 最后更新时间
     */
    private Date lastUpdateTime;

    /**
     * 内容视图
     */
    private String contentView;

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getGlobalTxId() {
        return globalTxId;
    }

    public void setGlobalTxId(String globalTxId) {
        this.globalTxId = globalTxId;
    }

    public String getBranchQualifier() {
        return branchQualifier;
    }

    public void setBranchQualifier(String branchQualifier) {
        this.branchQualifier = branchQualifier;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(Integer transactionType) {
        this.transactionType = transactionType;
    }

    public Integer getRetriedCount() {
        return retriedCount;
    }

    public void setRetriedCount(Integer retriedCount) {
        this.retriedCount = retriedCount;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getContentView() {
        return contentView;
    }

    public void setContentView(String contentView) {
        this.contentView = contentView;
    }
}