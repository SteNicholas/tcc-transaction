package org.mengyun.tcctransaction.sample.order.domain.entity;

/**
 * 商店
 */
public class Shop {

    /**
     * 商店编号
     */
    private long id;

    /**
     * 拥有者用户编号
     */
    private long ownerUserId;

    public long getOwnerUserId() {
        return ownerUserId;
    }

    public long getId() {
        return id;
    }
}