package org.mengyun.tcctransaction.sample.http.order.service;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.sample.http.capital.api.dto.CapitalTradeOrderDto;
import org.mengyun.tcctransaction.sample.http.redpacket.api.dto.RedPacketTradeOrderDto;
import org.mengyun.tcctransaction.sample.order.domain.entity.Order;
import org.mengyun.tcctransaction.sample.order.domain.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Calendar;

/**
 * 支付服务实现
 */
@Service
public class PaymentServiceImpl {

    @Autowired
    TradeOrderServiceProxy tradeOrderServiceProxy;

    @Autowired
    OrderRepository orderRepository;

    /**
     * 调用远程Tcc服务,将远程Tcc服务参与到本地Tcc事务中,本地的服务方法也需要声明为Tcc服务,与发布一个Tcc服务不同,本地Tcc服务方法有三个约束：
     * (1)在服务方法上加上@Compensable注解,并设置注解属性;
     * (2)服务方法的入参都须能序列化(实现Serializable接口);
     * (3)try方法、confirm方法和cancel方法入参类型须一样.
     * <p>
     * 下完订单后,订单状态为DRAFT,在TCC事务中TRY阶段,订单支付服务将订单状态变成PAYING,同时远程调用红包帐户服务和资金帐户服务,将付款方的余额减掉(预留业务资源);
     * 如果TRY阶段正常完成,则进入CONFIRM阶段,在CONFIRM阶段(tcc-transaction自动调用),订单支付服务将订单状态变成CONFIRMED,同时远程调用红包帐户服务和资金帐户服务对应的CONFIRM方法,将收款方的余额增加;
     * 如果在TRY阶段,任何一个服务失败,tcc-transaction将自动调用这些服务对应的cancel方法,订单支付服务将订单状态变成PAY_FAILED,同时远程调用红包帐户服务和资金帐户服务,将付款方余额减掉的部分增加回去.
     * <p>
     * 设置方法注解 @Transactional保证方法操作原子性.
     *
     * @param order
     * @param redPacketPayAmount
     * @param capitalPayAmount
     */
    @Compensable(confirmMethod = "confirmMakePayment", cancelMethod = "cancelMakePayment", asyncConfirm = true)
    @Transactional
    public void makePayment(Order order, BigDecimal redPacketPayAmount, BigDecimal capitalPayAmount) {
        System.out.println("order try make payment called.time seq:" + DateFormatUtils.format(Calendar.getInstance(), "yyyy-MM-dd HH:mm:ss"));

        //检查订单状态是否为草稿DRAFT
        //check if the order status is DRAFT, if no, means that another call makePayment for the same order happened, ignore this call makePayment.
        if (order.getStatus().equals("DRAFT")) {
            //更新订单红包支付金额、资金支付金额,订单状态为支付中PAYING
            order.pay(redPacketPayAmount, capitalPayAmount);
            try {
                orderRepository.updateOrder(order);
            } catch (OptimisticLockingFailureException e) {
                //ignore the concurrently update order exception, ensure idempotency.
            }
        }

        //发起资金账户余额支付订单
        String result = tradeOrderServiceProxy.record(null, buildCapitalTradeOrderDto(order));
        //发起红包账户余额支付订单
        String result2 = tradeOrderServiceProxy.record(null, buildRedPacketTradeOrderDto(order));
    }

    public void confirmMakePayment(Order order, BigDecimal redPacketPayAmount, BigDecimal capitalPayAmount) {
        try {
            Thread.sleep(1000l);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("order confirm make payment called. time seq:" + DateFormatUtils.format(Calendar.getInstance(), "yyyy-MM-dd HH:mm:ss"));

        Order foundOrder = orderRepository.findByMerchantOrderNo(order.getMerchantOrderNo());
        //check order status, only if the status equals DRAFT, then confirm order
        if (foundOrder != null && foundOrder.getStatus().equals("PAYING")) {
            //更新订单状态为支付成功CONFIRMED
            order.confirm();
            orderRepository.updateOrder(order);
        }
    }

    public void cancelMakePayment(Order order, BigDecimal redPacketPayAmount, BigDecimal capitalPayAmount) {
        try {
            Thread.sleep(1000l);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("order cancel make payment called.time seq:" + DateFormatUtils.format(Calendar.getInstance(), "yyyy-MM-dd HH:mm:ss"));

        Order foundOrder = orderRepository.findByMerchantOrderNo(order.getMerchantOrderNo());
        if (foundOrder != null && foundOrder.getStatus().equals("PAYING")) {
            //更新订单状态为支付失败PAY_FAILED
            order.cancelPayment();
            orderRepository.updateOrder(order);
        }
    }

    private CapitalTradeOrderDto buildCapitalTradeOrderDto(Order order) {
        CapitalTradeOrderDto tradeOrderDto = new CapitalTradeOrderDto();
        tradeOrderDto.setAmount(order.getCapitalPayAmount());
        tradeOrderDto.setMerchantOrderNo(order.getMerchantOrderNo());
        tradeOrderDto.setSelfUserId(order.getPayerUserId());
        tradeOrderDto.setOppositeUserId(order.getPayeeUserId());
        tradeOrderDto.setOrderTitle(String.format("order no:%s", order.getMerchantOrderNo()));

        return tradeOrderDto;
    }

    private RedPacketTradeOrderDto buildRedPacketTradeOrderDto(Order order) {
        RedPacketTradeOrderDto tradeOrderDto = new RedPacketTradeOrderDto();
        tradeOrderDto.setAmount(order.getRedPacketPayAmount());
        tradeOrderDto.setMerchantOrderNo(order.getMerchantOrderNo());
        tradeOrderDto.setSelfUserId(order.getPayerUserId());
        tradeOrderDto.setOppositeUserId(order.getPayeeUserId());
        tradeOrderDto.setOrderTitle(String.format("order no:%s", order.getMerchantOrderNo()));

        return tradeOrderDto;
    }
}