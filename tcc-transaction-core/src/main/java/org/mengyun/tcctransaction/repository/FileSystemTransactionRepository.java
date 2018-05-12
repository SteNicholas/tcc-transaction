package org.mengyun.tcctransaction.repository;

import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.common.TransactionType;
import org.mengyun.tcctransaction.repository.helper.TransactionSerializer;
import org.mengyun.tcctransaction.serializer.JdkSerializationSerializer;
import org.mengyun.tcctransaction.serializer.ObjectSerializer;

import javax.transaction.xa.Xid;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * File事务存储器,用于将事务存储到文件系统,不支持乐观锁更新
 * 仅适用事务发布方或调用方应用节点为单节点场景,因为日志是存储在应用节点本地文件中
 * 生产上不建议使用FileSystemTransactionRepository因为分布式存储挂载文件,不支持多节点共享
 * this repository is suitable for single node, not for cluster nodes
 */
public class FileSystemTransactionRepository extends CachableTransactionRepository {

    /**
     * 存储文件根目录
     */
    private String rootPath = "/tcc";

    /**
     * 根目录是否初始化
     */
    private volatile boolean initialized;

    private ObjectSerializer serializer = new JdkSerializationSerializer();

    public void setSerializer(ObjectSerializer serializer) {
        this.serializer = serializer;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    /**
     * 新增事务
     *
     * @param transaction
     * @return
     */
    @Override
    protected int doCreate(Transaction transaction) {
        //事务写入文件
        writeFile(transaction);
        return 1;
    }

    /**
     * 更新事务
     *
     * @param transaction
     * @return
     */
    @Override
    protected int doUpdate(Transaction transaction) {
        //设置事务最后更新时间
        transaction.updateVersion();
        //设置事务最新版本号
        transaction.updateTime();
        //事务写入文件
        writeFile(transaction);
        return 1;
    }

    /**
     * 删除事务
     *
     * @param transaction
     * @return
     */
    @Override
    protected int doDelete(Transaction transaction) {
        //根据事务编号获取文件路径
        String fullFileName = getFullFileName(transaction.getXid());
        File file = new File(fullFileName);
        if (file.exists()) {
            return file.delete() ? 1 : 0;
        }
        return 1;
    }

    /**
     * 根据事务编号获取事务
     *
     * @param xid
     * @return
     */
    @Override
    protected Transaction doFindOne(Xid xid) {
        String fullFileName = getFullFileName(xid);
        File file = new File(fullFileName);

        if (file.exists()) {
            return readTransaction(file);
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
        //获取文件系统存储的所有事务
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
     * 获取文件系统存储的所有事务
     *
     * @return
     */
    protected List<Transaction> doFindAll() {
        List<Transaction> transactions = new ArrayList<Transaction>();
        File path = new File(rootPath);
        File[] files = path.listFiles();

        for (File file : files) {
            Transaction transaction = readTransaction(file);
            transactions.add(transaction);
        }

        return transactions;
    }

    /**
     * 根据事务编号获取文件路径
     *
     * @param xid
     * @return
     */
    private String getFullFileName(Xid xid) {
        return String.format("%s/%s", rootPath, xid);
    }

    /**
     * 初始化根目录
     */
    private void makeDirIfNecessary() {
        if (!initialized) {
            synchronized (FileSystemTransactionRepository.class) {
                if (!initialized) {
                    File rootPathFile = new File(rootPath);
                    if (!rootPathFile.exists()) {
                        boolean result = rootPathFile.mkdir();

                        if (!result) {
                            throw new TransactionIOException("cannot create root path, the path to create is:" + rootPath);
                        }

                        initialized = true;
                    } else if (!rootPathFile.isDirectory()) {
                        throw new TransactionIOException("rootPath is not directory");
                    }
                }
            }
        }
    }

    /**
     * 事务写入文件
     *
     * @param transaction
     */
    private void writeFile(Transaction transaction) {
        makeDirIfNecessary();

        String file = getFullFileName(transaction.getXid());

        FileChannel channel = null;
        RandomAccessFile raf = null;

        byte[] content = TransactionSerializer.serialize(serializer, transaction);
        try {
            raf = new RandomAccessFile(file, "rw");
            channel = raf.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(content.length);
            buffer.put(content);
            buffer.flip();

            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            channel.force(true);
        } catch (Exception e) {
            throw new TransactionIOException(e);
        } finally {
            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new TransactionIOException(e);
                }
            }
        }
    }

    /**
     * 根据文件获取事务
     *
     * @param file
     * @return
     */
    private Transaction readTransaction(File file) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);

            byte[] content = new byte[(int) file.length()];
            fis.read(content);
            if (content != null) {
                return TransactionSerializer.deserialize(serializer, content);
            }
        } catch (Exception e) {
            throw new TransactionIOException(e);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    throw new TransactionIOException(e);
                }
            }
        }

        return null;
    }
}