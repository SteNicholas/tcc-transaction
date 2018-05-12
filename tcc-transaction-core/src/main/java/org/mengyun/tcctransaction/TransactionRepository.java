package org.mengyun.tcctransaction;

import org.mengyun.tcctransaction.api.TransactionXid;

import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * 事务存储器,用于持久化事务日志:
 * 1.JdbcTransactionRepository-JDBC事务存储器
 * 2.RedisTransactionRepository-Redis事务存储器
 * 3.ZooKeeperTransactionRepository-ZooKeeper事务存储器
 * 4.FileSystemTransactionRepository-File事务存储器
 * FileSystemTransactionRepository适合事务提供方节点或是事务调用方是单节点场景,RedisTransactionRepository、ZooKeeperTransactionRepository和JdbcTransactionRepository适合事务提供方节点或是事务调用方节点是多个节点场景(集群)
 */
public interface TransactionRepository {

    /**
     * 新增事务
     *
     * @param transaction
     * @return
     */
    int create(Transaction transaction);

    /**
     * 更新事务
     *
     * @param transaction
     * @return
     */
    int update(Transaction transaction);

    /**
     * 删除事务
     *
     * @param transaction
     * @return
     */
    int delete(Transaction transaction);

    /**
     * 根据事务编号获取事务
     *
     * @param xid
     * @return
     */
    Transaction findByXid(TransactionXid xid);

    /**
     * 获取超过指定时间的事务集合
     *
     * @param date
     * @return
     */
    List<Transaction> findAllUnmodifiedSince(Date date);
}