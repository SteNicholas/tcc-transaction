package org.mengyun.tcctransaction.repository;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.mengyun.tcctransaction.OptimisticLockException;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionRepository;
import org.mengyun.tcctransaction.api.TransactionXid;

import javax.transaction.xa.Xid;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 可缓存事务存储器,用于实现增删改查事务,缓存事务信息
 */
public abstract class CachableTransactionRepository implements TransactionRepository {
    /**
     * 缓存过期时间默认为120秒
     */
    private int expireDuration = 120;

    /**
     * 可补偿事务缓存
     */
    private Cache<Xid, Transaction> transactionXidCompensableTransactionCache;

    /**
     * 新增事务
     *
     * @param transaction
     * @return
     */
    @Override
    public int create(Transaction transaction) {
        //新增事务
        int result = doCreate(transaction);
        if (result > 0) {
            //添加事务到缓存
            putToCache(transaction);
        }
        return result;
    }

    /**
     * 更新事务
     *
     * @param transaction
     * @return
     */
    @Override
    public int update(Transaction transaction) {
        int result = 0;

        try {
            //更新事务
            result = doUpdate(transaction);
            if (result > 0) {
                //添加事务到缓存
                putToCache(transaction);
            } else {
                //更新事务失败抛出 OptimisticLockException异常,有两种情况会导致更新事务失败:
                // (1)该事务已经被提交,被删除;(2)乐观锁更新时,缓存的事务版本号(Transaction.version)和存储器里的事务版本号不同更新失败;
                // 更新事务失败,则事务缓存不一致,调用#removeFromCache(...)方法从缓存移除事务
                throw new OptimisticLockException();
            }
        } finally {
            if (result <= 0) {
                //从缓存移除事务
                removeFromCache(transaction);
            }
        }

        return result;
    }

    /**
     * 删除事务
     *
     * @param transaction
     * @return
     */
    @Override
    public int delete(Transaction transaction) {
        int result = 0;

        try {
            //删除事务
            result = doDelete(transaction);
        } finally {
            //从缓存移除事务
            removeFromCache(transaction);
        }
        return result;
    }

    /**
     * 根据事务编号获取事务
     *
     * @param transactionXid
     * @return
     */
    @Override
    public Transaction findByXid(TransactionXid transactionXid) {
        //优先根据事务编号从缓存获取事务
        Transaction transaction = findFromCache(transactionXid);

        //缓存中事务不存在,从存储器中获取,获取到事务调用#putToCache()方法添加事务到缓存
        if (transaction == null) {
            //根据事务编号查询事务
            transaction = doFindOne(transactionXid);

            if (transaction != null) {
                //添加事务到缓存
                putToCache(transaction);
            }
        }

        return transaction;
    }

    /**
     * 获取超过指定时间的事务集合
     *
     * @param date
     * @return
     */
    @Override
    public List<Transaction> findAllUnmodifiedSince(Date date) {
        //获取超过指定时间的事务集合
        List<Transaction> transactions = doFindAllUnmodifiedSince(date);

        //遍历事务集合循环添加事务到缓存
        for (Transaction transaction : transactions) {
            putToCache(transaction);
        }

        return transactions;
    }

    /**
     * 可缓存事务存储器构造方法,使用 Guava Cache内存缓存事务信息,默认设置最大缓存个数为 1000个,缓存过期时间为最后访问时间 120 秒
     */
    public CachableTransactionRepository() {
        transactionXidCompensableTransactionCache = CacheBuilder.newBuilder().expireAfterAccess(expireDuration, TimeUnit.SECONDS).maximumSize(1000).build();
    }

    /**
     * 添加事务到缓存
     *
     * @param transaction
     */
    protected void putToCache(Transaction transaction) {
        transactionXidCompensableTransactionCache.put(transaction.getXid(), transaction);
    }

    /**
     * 从缓存移除事务
     *
     * @param transaction
     */
    protected void removeFromCache(Transaction transaction) {
        transactionXidCompensableTransactionCache.invalidate(transaction.getXid());
    }

    /**
     * 根据事务编号从缓存获取事务
     *
     * @param transactionXid
     * @return
     */
    protected Transaction findFromCache(TransactionXid transactionXid) {
        return transactionXidCompensableTransactionCache.getIfPresent(transactionXid);
    }

    public void setExpireDuration(int durationInSeconds) {
        this.expireDuration = durationInSeconds;
    }

    /**
     * 新增事务
     *
     * @param transaction
     * @return
     */
    protected abstract int doCreate(Transaction transaction);

    /**
     * 更新事务
     *
     * @param transaction
     * @return
     */
    protected abstract int doUpdate(Transaction transaction);

    /**
     * 删除事务
     *
     * @param transaction
     * @return
     */
    protected abstract int doDelete(Transaction transaction);

    /**
     * 根据事务编号查询事务
     *
     * @param xid
     * @return
     */
    protected abstract Transaction doFindOne(Xid xid);

    /**
     * 获取超过指定时间的事务集合
     *
     * @param date
     * @return
     */
    protected abstract List<Transaction> doFindAllUnmodifiedSince(Date date);
}