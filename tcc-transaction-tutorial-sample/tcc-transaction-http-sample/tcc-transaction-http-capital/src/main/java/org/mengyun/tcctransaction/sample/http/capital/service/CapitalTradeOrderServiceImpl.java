package org.mengyun.tcctransaction.sample.http.capital.service;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.context.MethodTransactionContextEditor;
import org.mengyun.tcctransaction.sample.capital.domain.entity.CapitalAccount;
import org.mengyun.tcctransaction.sample.capital.domain.entity.TradeOrder;
import org.mengyun.tcctransaction.sample.capital.domain.repository.CapitalAccountRepository;
import org.mengyun.tcctransaction.sample.capital.domain.repository.TradeOrderRepository;
import org.mengyun.tcctransaction.sample.http.capital.api.CapitalTradeOrderService;
import org.mengyun.tcctransaction.sample.http.capital.api.dto.CapitalTradeOrderDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Transactional;

import java.util.Calendar;

/**
 * 资金交易订单服务实现
 */
public class CapitalTradeOrderServiceImpl implements CapitalTradeOrderService {

    @Autowired
    CapitalAccountRepository capitalAccountRepository;

    @Autowired
    TradeOrderRepository tradeOrderRepository;

    /**
     * Try:尝试执行业务:(1)完成所有业务检查(一致性);(2)预留必须业务资源(准隔离性)
     * Confirm:确认执行业务:(1)真正执行业务;(2)不作任何业务检查;(3)只使用Try阶段预留的业务资源;(4)Confirm操作满足幂等性
     * Cancel:取消执行业务:(1)释放Try阶段预留的业务资源;(2)Cancel操作满足幂等性
     * <p>
     * 发布一个Tcc服务方法,可被远程调用并参与到Tcc事务中,发布Tcc服务方法有下面四个约束：
     * (1)在服务提供方的实现方法上加上@Compensable注解,并设置注解的属性;
     * (2)服务方法第一个入参类型为org.mengyun.tcctransaction.api.TransactionContext;
     * (3)服务方法的入参能被序列化(默认使用jdk序列化机制,需要参数实现Serializable接口,可以设置repository的serializer属性自定义序列化实现)
     * (4)try方法、confirm方法和cancel方法入参类型须一样.
     * <p>
     * Compensable的属性包括propagation、confirmMethod、cancelMethod、transactionContextEditor：
     * (1)propagation可不用设置,框架使用缺省值;
     * (2)设置confirmMethod指定CONFIRM阶段的调用方法;
     * (3)设置cancelMethod指定CANCEL阶段的调用方法;
     * (4)设置transactionContextEditor为MethodTransactionContextEditor.class,
     * tcc-transaction框架使用transactionContextEditor来实现获取和调用方向服务提供方传递TransactionContext.
     * <p>
     * tcc-transaction将拦截加上了@Compensable注解的服务方法,并根据Compensalbe的confirmMethod和cancelMethod获取在CONFRIM阶段和CANCEL阶段需要调用的方法.
     * 注解属性tcc-transaction在调用confirmMethod或是cancelMethod时是根据发布Tcc服务的接口类在Spring的ApplicationContext中获取Tcc服务实例,并调用confirmMethod或cancelMethod指定方法.
     * 如果是使用动态代理的方式实现aop(默认方式),则confirmMethod和cancelMethod需在接口类中声明,如果使用动态字节码技术实现aop.则无需在接口类中声明.
     * <p>
     * tcc-transaction在执行服务过程中会将Tcc服务的上下文持久化,包括所有入参,内部默认实现为将入参使用jdk自带的序列化机制序列化为为byte流,所以需要实现Serializable接口.
     * <p>
     * 设置方法注解 @Transactional保证方法操作原子性.
     *
     * @param transactionContext
     * @param tradeOrderDto
     * @return
     */
    @Override
    @Compensable(confirmMethod = "confirmRecord", cancelMethod = "cancelRecord", transactionContextEditor = MethodTransactionContextEditor.class)
    @Transactional
    public String record(TransactionContext transactionContext, CapitalTradeOrderDto tradeOrderDto) {
        try {
            Thread.sleep(1000l);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("capital try record called. time seq:" + DateFormatUtils.format(Calendar.getInstance(), "yyyy-MM-dd HH:mm:ss"));

        TradeOrder foundTradeOrder = tradeOrderRepository.findByMerchantOrderNo(tradeOrderDto.getMerchantOrderNo());
        //check if trade order has been recorded, if yes, return success directly.
        if (foundTradeOrder == null) {
            //创建交易订单
            TradeOrder tradeOrder = new TradeOrder(
                    tradeOrderDto.getSelfUserId(),
                    tradeOrderDto.getOppositeUserId(),
                    tradeOrderDto.getMerchantOrderNo(),
                    tradeOrderDto.getAmount()
            );

            try {
                tradeOrderRepository.insert(tradeOrder);

                CapitalAccount transferFromAccount = capitalAccountRepository.findByUserId(tradeOrderDto.getSelfUserId());
                //扣减下单用户的资金账户余额,Try阶段必须事先扣减金额,预留必须业务资源
                transferFromAccount.transferFrom(tradeOrderDto.getAmount());
                capitalAccountRepository.save(transferFromAccount);
            } catch (DataIntegrityViolationException e) {
                //this exception may happen when insert trade order concurrently, if happened, ignore this insert operation.
            }
        }

        return "success";
    }

    @Transactional
    public void confirmRecord(TransactionContext transactionContext, CapitalTradeOrderDto tradeOrderDto) {
        try {
            Thread.sleep(1000l);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("capital confirm record called. time seq:" + DateFormatUtils.format(Calendar.getInstance(), "yyyy-MM-dd HH:mm:ss"));

        //查询交易订单
        TradeOrder tradeOrder = tradeOrderRepository.findByMerchantOrderNo(tradeOrderDto.getMerchantOrderNo());
        //判断交易订单状态是否为草稿DRAFT,因为 record()方法可能回滚事务,交易订单不存在/交易订单状态错误
        //check if the trade order status is DRAFT, if yes, return directly, ensure idempotency.
        if (null != tradeOrder && "DRAFT".equals(tradeOrder.getStatus())) {
            //更新订单状态为支付成功CONFIRMED
            tradeOrder.confirm();
            tradeOrderRepository.update(tradeOrder);

            //增加转入用户[商店拥有者用户]的资金账户余额
            CapitalAccount transferToAccount = capitalAccountRepository.findByUserId(tradeOrderDto.getOppositeUserId());
            transferToAccount.transferTo(tradeOrderDto.getAmount());
            capitalAccountRepository.save(transferToAccount);
        }
    }

    @Transactional
    public void cancelRecord(TransactionContext transactionContext, CapitalTradeOrderDto tradeOrderDto) {
        try {
            Thread.sleep(1000l);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("capital cancel record called. time seq:" + DateFormatUtils.format(Calendar.getInstance(), "yyyy-MM-dd HH:mm:ss"));

        //查询交易订单
        TradeOrder tradeOrder = tradeOrderRepository.findByMerchantOrderNo(tradeOrderDto.getMerchantOrderNo());
        //判断交易订单状态是否为草稿DRAFT,因为 record()方法可能回滚事务,交易订单不存在/交易订单状态错误
        //check if the trade order status is DRAFT, if yes, return directly, ensure idempotency.
        if (null != tradeOrder && "DRAFT".equals(tradeOrder.getStatus())) {
            //更新订单状态为支付失败PAY_FAILED
            tradeOrder.cancel();
            tradeOrderRepository.update(tradeOrder);

            //增加(恢复)转出用户[下单用户]的资金账户余额
            CapitalAccount capitalAccount = capitalAccountRepository.findByUserId(tradeOrderDto.getSelfUserId());
            capitalAccount.cancelTransfer(tradeOrderDto.getAmount());
            capitalAccountRepository.save(capitalAccount);
        }
    }
}