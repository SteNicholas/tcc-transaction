package org.mengyun.tcctransaction.repository;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.common.TransactionType;
import org.mengyun.tcctransaction.repository.helper.TransactionSerializer;
import org.mengyun.tcctransaction.serializer.JdkSerializationSerializer;
import org.mengyun.tcctransaction.serializer.ObjectSerializer;

import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * ZooKeeper事务存储器,用于将事务存储到Zookeeper,不支持乐观锁更新
 * 生产上不建议使用ZooKeeperTransactionRepository,原因有两点:1.不支持 ZooKeeper安全认证;2.使用ZooKeeper时,未考虑断网重连等情况
 * 使用 ZooKeeper进行事务的存储考虑使用Apache Curator操作 Zookeeper,重写 ZooKeeperTransactionRepository
 */
public class ZooKeeperTransactionRepository extends CachableTransactionRepository {

    /**
     * ZooKeeper服务器地址
     */
    private String zkServers;

    /**
     * ZooKeeper超时时间
     */
    private int zkTimeout;

    /**
     * ZooKeeper根目录路径
     */
    private String zkRootPath = "/tcc";

    /**
     * ZooKeeper连接
     */
    private volatile ZooKeeper zk;

    /**
     * 序列化
     */
    private ObjectSerializer serializer = new JdkSerializationSerializer();

    public ZooKeeperTransactionRepository() {
        super();
    }

    public void setSerializer(ObjectSerializer serializer) {
        this.serializer = serializer;
    }

    public void setZkRootPath(String zkRootPath) {
        this.zkRootPath = zkRootPath;
    }

    public void setZkServers(String zkServers) {
        this.zkServers = zkServers;
    }

    public void setZkTimeout(int zkTimeout) {
        this.zkTimeout = zkTimeout;
    }

    /**
     * 新增事务
     *
     * @param transaction
     * @return
     */
    @Override
    protected int doCreate(Transaction transaction) {
        try {
            getZk().create(getTxidPath(transaction.getXid()),
                    TransactionSerializer.serialize(serializer, transaction), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return 1;
        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
    }

    /**
     * 更新事务
     *
     * @param transaction
     * @return
     */
    @Override
    protected int doUpdate(Transaction transaction) {
        try {
            //设置事务最后更新时间
            transaction.updateTime();
            //设置事务最新版本号
            transaction.updateVersion();

            //-2的原因是Transaction版本从1开始,Zookeeper数据节点版本从0开始,调用transaction.updateVersion()版本号+1
            Stat stat = getZk().setData(getTxidPath(transaction.getXid()), TransactionSerializer.serialize(serializer, transaction), (int) transaction.getVersion() - 2);
            return 1;
        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
    }

    /**
     * 删除事务
     *
     * @param transaction
     * @return
     */
    @Override
    protected int doDelete(Transaction transaction) {
        try {
            //-1的原因是Transaction版本从1开始,Zookeeper数据节点版本从0开始
            getZk().delete(getTxidPath(transaction.getXid()), (int) transaction.getVersion() - 1);
            return 1;
        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
    }

    /**
     * 根据事务编号获取事务
     *
     * @param xid
     * @return
     */
    @Override
    protected Transaction doFindOne(Xid xid) {
        byte[] content = null;
        try {
            Stat stat = new Stat();
            content = getZk().getData(getTxidPath(xid), false, stat);
            Transaction transaction = TransactionSerializer.deserialize(serializer, content);

            return transaction;
        } catch (KeeperException.NoNodeException e) {

        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
        return null;
    }

    /**
     * 获取超过指定时间的事务集合
     *
     * @param date
     * @return
     */
    @Override
    protected List<Transaction> doFindAllUnmodifiedSince(Date date) {
        //获取ZooKeeper存储的所有事务
        List<Transaction> allTransactions = doFindAll();
        //加载所有事务根据时间过滤
        List<Transaction> allUnmodifiedSince = new ArrayList<Transaction>();
        for (Transaction transaction : allTransactions) {
            if (transaction.getLastUpdateTime().compareTo(date) < 0) {
                allUnmodifiedSince.add(transaction);
            }
        }

        return allUnmodifiedSince;
    }

    /**
     * 获取ZooKeeper存储的所有事务
     *
     * @return
     */
    protected List<Transaction> doFindAll() {
        List<Transaction> transactions = new ArrayList<Transaction>();

        List<String> znodePaths = null;
        try {
            //获取ZooKeeper根目录${zkRootPath}下所有的数据节点即事务
            znodePaths = getZk().getChildren(zkRootPath, false);
        } catch (Exception e) {
            throw new TransactionIOException(e);
        }

        for (String znodePath : znodePaths) {
            byte[] content = null;
            try {
                Stat stat = new Stat();
                content = getZk().getData(getTxidPath(znodePath), false, stat);
                Transaction transaction = TransactionSerializer.deserialize(serializer, content);
                transactions.add(transaction);
            } catch (Exception e) {
                throw new TransactionIOException(e);
            }
        }

        return transactions;
    }

    /**
     * 获取ZooKeeper连接
     *
     * @return
     */
    private ZooKeeper getZk() {
        if (zk == null) {
            synchronized (ZooKeeperTransactionRepository.class) {
                if (zk == null) {
                    try {
                        //创建ZooKeeper连接
                        zk = new ZooKeeper(zkServers, zkTimeout, new Watcher() {
                            @Override
                            public void process(WatchedEvent watchedEvent) {

                            }
                        });

                        //创建ZooKeeper根目录
                        Stat stat = zk.exists(zkRootPath, false);
                        if (stat == null) {
                            zk.create(zkRootPath, zkRootPath.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        }
                    } catch (Exception e) {
                        throw new TransactionIOException(e);
                    }
                }
            }
        }
        return zk;
    }

    /**
     * 根据事务编号获取事务路径
     *
     * @param xid
     * @return
     */
    private String getTxidPath(Xid xid) {
        return String.format("%s/%s", zkRootPath, xid);
    }

    /**
     * 根据ZNode路径获取事务路径
     *
     * @param znodePath
     * @return
     */
    private String getTxidPath(String znodePath) {
        return String.format("%s/%s", zkRootPath, znodePath);
    }
}