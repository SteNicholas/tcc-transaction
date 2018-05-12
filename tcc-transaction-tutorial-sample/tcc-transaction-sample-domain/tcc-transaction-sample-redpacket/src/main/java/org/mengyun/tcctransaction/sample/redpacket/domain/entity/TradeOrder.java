package org.mengyun.tcctransaction.sample.redpacket.domain.entity;

import java.math.BigDecimal;

/**
 * 交易订单:
 * (1)订单支付时候,插入交易订单状态为 DRAFT的订单记录,并且更新扣减下单用户的红包账户余额;
 * (2)订单支付成功,更新交易订单状态为 CONFIRM,并且更新增加商店拥有用户的红包账户余额;
 * (3)订单支付失败,更新交易订单状态为 CANCEL,并且更新**增加( 恢复 )**下单用户的红包账户余额
 */
public class TradeOrder {

    /**
     * 交易订单编号
     */
    private long id;

    /**
     * 转出用户编号
     */
    private long selfUserId;

    /**
     * 转入用户编号
     */
    private long oppositeUserId;

    /**
     * 商家订单编号
     */
    private String merchantOrderNo;

    /**
     * 金额
     */
    private BigDecimal amount;

    /**
     * 交易订单状态:DRAFT[草稿]/CONFIRM[交易成功]/CANCEL[交易取消]
     */
    private String status = "DRAFT";

    private long version = 1l;

    public TradeOrder() {
    }

    public TradeOrder(long selfUserId, long oppositeUserId, String merchantOrderNo, BigDecimal amount) {
        this.selfUserId = selfUserId;
        this.oppositeUserId = oppositeUserId;
        this.merchantOrderNo = merchantOrderNo;
        this.amount = amount;
    }

    public long getId() {
        return id;
    }

    public long getSelfUserId() {
        return selfUserId;
    }

    public long getOppositeUserId() {
        return oppositeUserId;
    }

    public String getMerchantOrderNo() {
        return merchantOrderNo;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getStatus() {
        return status;
    }

    public void confirm() {
        this.status = "CONFIRM";
    }

    public void cancel() {
        this.status = "CANCEL";
    }

    public long getVersion() {
        return version;
    }

    public void updateVersion() {
        version = version + 1;
    }
}