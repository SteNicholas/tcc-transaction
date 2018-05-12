package org.mengyun.tcctransaction.sample.order.domain.service;

import org.apache.commons.lang3.tuple.Pair;
import org.mengyun.tcctransaction.sample.order.domain.entity.Order;
import org.mengyun.tcctransaction.sample.order.domain.factory.OrderFactory;
import org.mengyun.tcctransaction.sample.order.domain.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 订单服务实现
 */
@Service
public class OrderServiceImpl {

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    OrderFactory orderFactory;

    /**
     * 创建订单
     *
     * @param payerUserId
     * @param payeeUserId
     * @param productQuantities
     * @return
     */
    @Transactional
    public Order createOrder(long payerUserId, long payeeUserId, List<Pair<Long, Integer>> productQuantities) {
        Order order = orderFactory.buildOrder(payerUserId, payeeUserId, productQuantities);
        orderRepository.createOrder(order);
        return order;
    }

    /**
     *查询订单
     *
     * @param orderNo
     * @return
     */
    public Order findOrderByMerchantOrderNo(String orderNo) {
        return orderRepository.findByMerchantOrderNo(orderNo);
    }
}