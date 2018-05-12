package org.mengyun.tcctransaction.sample.order.domain.entity;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 商品
 */
public class Product implements Serializable {

    /**
     * 商品编号
     */
    private long productId;

    /**
     * 商店编号
     */
    private long shopId;

    /**
     * 商品名称
     */
    private String productName;

    /**
     * 商品价格
     */
    private BigDecimal price;

    public Product() {
    }

    public Product(long productId, long shopId, String productName, BigDecimal price) {
        this.productId = productId;
        this.shopId = shopId;
        this.productName = productName;
        this.price = price;
    }

    public long getProductId() {
        return productId;
    }

    public long getShopId() {
        return shopId;
    }

    public String getProductName() {
        return productName;
    }

    public BigDecimal getPrice() {
        return price;
    }
}