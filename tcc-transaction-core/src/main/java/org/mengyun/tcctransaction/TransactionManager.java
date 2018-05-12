package org.mengyun.tcctransaction;

import org.apache.log4j.Logger;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.TransactionType;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

/**
 * 事务管理器
 */
public class TransactionManager {

    static final Logger logger = Logger.getLogger(TransactionManager.class.getSimpleName());

    /**
     * 事务存储器,用于持久化事务日志
     */
    private TransactionRepository transactionRepository;

    /**
     * 线程局部事务队列,是ThreadLocal队列,用于保存事务管理器活动的事务
     */
    private static final ThreadLocal<Deque<Transaction>> CURRENT = new ThreadLocal<Deque<Transaction>>();

    private ExecutorService executorService;

    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public TransactionManager() {
    }

    /**
     * 开始事务,发起根事务,通常在根事务的Try阶段被调用,在调用方法类型为 MethodType.ROOT并且事务处于 Try 阶段被调用
     *
     * @return
     */
    public Transaction begin() {
        //根据指定事务类型创建事务,事务类型为根事务ROOT
        Transaction transaction = new Transaction(TransactionType.ROOT);
        //事务存储器存储事务,事务日志创建事务
        transactionRepository.create(transaction);
        //注册事务到线程局部事务队列
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 从事务上下文中传播新事务,传播发起分支事务,通常在分支事务的Try阶段被调用,
     * 在调用方法类型为 MethodType.PROVIDER并且事务处于 Try 阶段被调用
     *
     * @param transactionContext
     * @return
     */
    public Transaction propagationNewBegin(TransactionContext transactionContext) {
        //根据事务上下文创建事务,事务类型为分支事务BRANCH
        Transaction transaction = new Transaction(transactionContext);
        //事务存储器存储事务,事务日志创建事务
        transactionRepository.create(transaction);
        //注册事务到线程局部事务队列
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 从事务上下文中传播已存在事务,传播获取分支事务,通常在分支事务的Confirm/Cancel阶段被调用,
     * 在调用方法类型为 MethodType.PROVIDER并且事务处于 Confirm/Cancel阶段被调用
     *
     * @param transactionContext
     * @return
     * @throws NoExistedTransactionException
     */
    public Transaction propagationExistBegin(TransactionContext transactionContext) throws NoExistedTransactionException {
        //根据事务上下文事务编号查询事务存储器持久化日志获取事务
        Transaction transaction = transactionRepository.findByXid(transactionContext.getXid());
        //判断事务存储器是否存在指定编号事务,是则更新事务状态注册事务,否则抛不存在事务异常
        if (transaction != null) {
            //更新事务状态为事务上下文事务状态,设置事务状态为 CONFIRMING或者CANCELLING
            transaction.changeStatus(TransactionStatus.valueOf(transactionContext.getStatus()));
            //注册事务到线程局部事务队列
            registerTransaction(transaction);
            return transaction;
        } else {
            throw new NoExistedTransactionException();
        }
    }

    /**
     * 提交事务:Commit在事务Try阶段无异常的情况调用,
     * (1)从线程局部事务ThreadLocal队列获取当前需要处理的事务;
     * (2)将事务状态更新为CONFIRMING状态,事务日志更新事务;
     * (3)调用事务的Commit方法执行事务提交处理,
     * 事务提交成功(无抛出任何异常),则从事务日志仓库删除事务,
     * 事务提交过程抛出异常,则事物日志此时不会删除事务(稍后会被Recovery恢复任务处理),同时转化异常为ConfirmingException抛出异常
     *
     * @param asyncCommit
     */
    public void commit(boolean asyncCommit) {
        //获取线程局部事务队列头部事务
        final Transaction transaction = getCurrentTransaction();
        //更改事务状态为CONFIRMING
        transaction.changeStatus(TransactionStatus.CONFIRMING);
        //事务存储器更新事务,事务日志更新事务
        transactionRepository.update(transaction);

        if (asyncCommit) {
            try {
                Long statTime = System.currentTimeMillis();

                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        //提交事务
                        commitTransaction(transaction);
                    }
                });
                logger.debug("async submit cost time:" + (System.currentTimeMillis() - statTime));
            } catch (Throwable commitException) {
                logger.warn("compensable transaction async submit confirm failed, recovery job will try to confirm later.", commitException);
                throw new ConfirmingException(commitException);
            }
        } else {
            //提交事务
            commitTransaction(transaction);
        }
    }

    /**
     * 同步事务
     */
    public void syncTransaction() {
        //获取线程局部事务队列头部事务
        final Transaction transaction = getCurrentTransaction();
        /**
         * update the transaction to persist the participant context info
         */
        //事务存储器更新事务,事务日志更新事务,持久化参与者上下文信息
        transactionRepository.update(transaction);
    }

    /**
     * 回滚事务:Rollback在事务Try阶段抛出异常的情况调用,
     * (1)从线程局部事务ThreadLocal队列获取当前需要处理的事务;
     * (2)将事务状态更新为CANCELLING状态,事务日志更新事务;
     * (3)调用事务的Rollback方法执行事务回滚处理,
     * 事务回滚成功(无抛出任何异常),则从事务日志仓库删除事务,
     * 事务回滚过程抛出异常,则事物日志此时不会删除事务(稍后会被Recovery恢复任务处理),同时转化异常为CancellingException抛出异常
     *
     * @param asyncRollback
     */
    public void rollback(boolean asyncRollback) {
        //获取线程局部事务队列头部事务
        final Transaction transaction = getCurrentTransaction();
        //更改事务状态为CANCELLING
        transaction.changeStatus(TransactionStatus.CANCELLING);
        //事务存储器更新事务,事务日志更新事务
        transactionRepository.update(transaction);

        if (asyncRollback) {
            try {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        //回滚事务
                        rollbackTransaction(transaction);
                    }
                });
            } catch (Throwable rollbackException) {
                logger.warn("compensable transaction async rollback failed, recovery job will try to rollback later.", rollbackException);
                throw new CancellingException(rollbackException);
            }
        } else {
            //回滚事务
            rollbackTransaction(transaction);
        }
    }

    /**
     * 提交事务
     *
     * @param transaction
     */
    private void commitTransaction(Transaction transaction) {
        try {
            //提交事务
            transaction.commit();
            //事务存储器删除事务,事务日志删除事务
            transactionRepository.delete(transaction);
        } catch (Throwable commitException) {
            //提交事务过程引起异常
            logger.warn("compensable transaction confirm failed, recovery job will try to confirm later.", commitException);
            //抛出ConfirmingException异常,导致事务日志不会删除事务,Recovery恢复策略处理长时间没有被删除的事务
            throw new ConfirmingException(commitException);
        }
    }

    /**
     * 回滚事务
     *
     * @param transaction
     */
    private void rollbackTransaction(Transaction transaction) {
        try {
            //回滚事务
            transaction.rollback();
            //事务存储器删除事务,事务日志删除事务
            transactionRepository.delete(transaction);
        } catch (Throwable rollbackException) {
            //回滚事务过程引起异常
            logger.warn("compensable transaction rollback failed, recovery job will try to rollback later.", rollbackException);
            //抛出CancellingException异常,导致事务日志不会删除事务,Recovery恢复策略处理长时间没有被删除的事务
            throw new CancellingException(rollbackException);
        }
    }

    /**
     * 获取线程局部事务队列头部事务
     *
     * @return
     */
    public Transaction getCurrentTransaction() {
        if (isTransactionActive()) {
            //从线程局部事务队列头部元素获取当前需要处理的事务
            return CURRENT.get().peek();
        }
        return null;
    }

    /**
     * 判断线程局部事务队列事务是否开启即当前线程是否在事务中
     *
     * @return
     */
    public boolean isTransactionActive() {
        //通过线程局部ThreadLocal变量获取线程局部事务队列
        Deque<Transaction> transactions = CURRENT.get();
        return transactions != null && !transactions.isEmpty();
    }

    /**
     * 注册事务到线程局部事务队列
     *
     * @param transaction
     */
    private void registerTransaction(Transaction transaction) {
        //判断线程局部事务队列是否创建,否则创建队列
        if (CURRENT.get() == null) {
            CURRENT.set(new LinkedList<Transaction>());
        }
        //事务添加到线程局部事务队列头部
        CURRENT.get().push(transaction);
    }

    /**
     * 事务处理结束清理事务
     *
     * @param transaction
     */
    public void cleanAfterCompletion(Transaction transaction) {
        if (isTransactionActive() && transaction != null) {
            //获取线程局部事务队列头部事务
            Transaction currentTransaction = getCurrentTransaction();
            if (currentTransaction == transaction) {
                //线程局部事务队列移除头部事务
                CURRENT.get().pop();
            } else {
                throw new SystemException("Illegal transaction when clean after completion");
            }
        }
    }

    /**
     * 事务添加参与者
     *
     * @param participant
     */
    public void enlistParticipant(Participant participant) {
        //获取线程局部事务队列头部事务
        Transaction transaction = this.getCurrentTransaction();
        //事务参与者集合添加参与者
        transaction.enlistParticipant(participant);
        //事务存储器更新事务,事务日志更新事务
        transactionRepository.update(transaction);
    }
}