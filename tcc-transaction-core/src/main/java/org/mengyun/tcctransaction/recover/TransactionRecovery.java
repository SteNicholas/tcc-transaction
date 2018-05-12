package org.mengyun.tcctransaction.recover;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.mengyun.tcctransaction.OptimisticLockException;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionRepository;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.TransactionType;
import org.mengyun.tcctransaction.support.TransactionConfigurator;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 异常事务恢复
 */
public class TransactionRecovery {

    static final Logger logger = Logger.getLogger(TransactionRecovery.class.getSimpleName());

    private TransactionConfigurator transactionConfigurator;

    /**
     * 启动事务恢复
     */
    public void startRecover() {
        //加载异常事务集合
        List<Transaction> transactions = loadErrorTransactions();
        //恢复异常事务集合
        recoverErrorTransactions(transactions);
    }

    /**
     * 加载异常事务集合
     *
     * @return
     */
    private List<Transaction> loadErrorTransactions() {
        long currentTimeInMillis = Calendar.getInstance().getTimeInMillis();

        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
        RecoverConfig recoverConfig = transactionConfigurator.getRecoverConfig();
        //异常事务的定义:当前时间超过-事务变更时间(最后执行时间 )>=事务恢复间隔,已完成的事务从事务存储器删除
        return transactionRepository.findAllUnmodifiedSince(new Date(currentTimeInMillis - recoverConfig.getRecoverDuration() * 1000));
    }

    private void recoverErrorTransactions(List<Transaction> transactions) {
        for (Transaction transaction : transactions) {
            //判断事务重试次数是否大于事务恢复策略配置的事务最多重试次数
            if (transaction.getRetriedCount() > transactionConfigurator.getRecoverConfig().getMaxRetryCount()) {
                logger.error(String.format("recover failed with max retry count,will not try again. txid:%s, status:%s,retried count:%d,transaction content:%s", transaction.getXid(), transaction.getStatus().getId(), transaction.getRetriedCount(), JSON.toJSONString(transaction)));
                continue;
            }

            //判断分支事务是否超过最大可重试时间
            if (transaction.getTransactionType().equals(TransactionType.BRANCH)
                    && (transaction.getCreateTime().getTime() +
                    transactionConfigurator.getRecoverConfig().getMaxRetryCount() *
                            transactionConfigurator.getRecoverConfig().getRecoverDuration() * 1000
                    > System.currentTimeMillis())) {
                continue;
            }
            
            try {
                //增加事务重试次数
                transaction.addRetriedCount();

                if (transaction.getStatus().equals(TransactionStatus.CONFIRMING)) {
                    //事务状态为TransactionStatus.CONFIRMING状态提交事务
                    transaction.changeStatus(TransactionStatus.CONFIRMING);
                    transactionConfigurator.getTransactionRepository().update(transaction);
                    transaction.commit();
                    transactionConfigurator.getTransactionRepository().delete(transaction);
                } else if (transaction.getStatus().equals(TransactionStatus.CANCELLING)
                        || transaction.getTransactionType().equals(TransactionType.ROOT)) {
                    //事务状态为TransactionStatus.CANCELLING状态事务类型为根事务(用于处理延迟回滚异常的事务的回滚)回滚事务
                    transaction.changeStatus(TransactionStatus.CANCELLING);
                    transactionConfigurator.getTransactionRepository().update(transaction);
                    transaction.rollback();
                    transactionConfigurator.getTransactionRepository().delete(transaction);
                }
            } catch (Throwable throwable) {
                if (throwable instanceof OptimisticLockException
                        || ExceptionUtils.getRootCause(throwable) instanceof OptimisticLockException) {
                    logger.warn(String.format("optimisticLockException happened while recover. txid:%s, status:%s,retried count:%d,transaction content:%s", transaction.getXid(), transaction.getStatus().getId(), transaction.getRetriedCount(), JSON.toJSONString(transaction)), throwable);
                } else {
                    logger.error(String.format("recover failed, txid:%s, status:%s,retried count:%d,transaction content:%s", transaction.getXid(), transaction.getStatus().getId(), transaction.getRetriedCount(), JSON.toJSONString(transaction)), throwable);
                }
            }
        }
    }

    public void setTransactionConfigurator(TransactionConfigurator transactionConfigurator) {
        this.transactionConfigurator = transactionConfigurator;
    }
}