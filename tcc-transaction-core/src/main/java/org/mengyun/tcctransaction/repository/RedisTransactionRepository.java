package org.mengyun.tcctransaction.repository;

import org.apache.log4j.Logger;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.repository.helper.ExpandTransactionSerializer;
import org.mengyun.tcctransaction.repository.helper.JedisCallback;
import org.mengyun.tcctransaction.repository.helper.RedisHelper;
import org.mengyun.tcctransaction.serializer.JdkSerializationSerializer;
import org.mengyun.tcctransaction.serializer.ObjectSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import javax.transaction.xa.Xid;
import java.util.*;

/**
 * Redis事务存储器,用于将事务存储到Redis,需配置redis服务器为AOF模式并在redis.conf中设置appendfsync为always以防止日志丢失
 * <p/>
 * As the storage of transaction need safely durable,make sure the redis server is set as AOF mode and always fsync.
 * set below directives in your redis.conf
 * appendonly yes
 * appendfsync always
 */
public class RedisTransactionRepository extends CachableTransactionRepository {

    static final Logger logger = Logger.getLogger(RedisTransactionRepository.class.getSimpleName());

    /**
     * Jedis池
     */
    private JedisPool jedisPool;

    /**
     * Key前缀
     */
    private String keyPrefix = "TCC:";

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    /**
     * 序列化
     */
    private ObjectSerializer serializer = new JdkSerializationSerializer();

    public void setSerializer(ObjectSerializer serializer) {
        this.serializer = serializer;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * 新增事务
     *
     * @param transaction
     * @return
     */
    @Override
    protected int doCreate(final Transaction transaction) {
        try {
            Long statusCode = RedisHelper.execute(jedisPool, new JedisCallback<Long>() {

                @Override
                public Long doInJedis(Jedis jedis) {
                    List<byte[]> params = new ArrayList<byte[]>();

                    for (Map.Entry<byte[], byte[]> entry : ExpandTransactionSerializer.serialize(serializer, transaction).entrySet()) {
                        params.add(entry.getKey());
                        params.add(entry.getValue());
                    }

                    //根据Key前缀、事务编号获取Redis Key,使用 Redis HSETNX添加事务,不存在值则进行设置
                    Object result = jedis.eval("if redis.call('exists', KEYS[1]) == 0 then redis.call('hmset', KEYS[1], unpack(ARGV)); return 1; end; return 0;".getBytes(),
                            Arrays.asList(RedisHelper.getRedisKey(keyPrefix, transaction.getXid())), params);

                    return (Long) result;
                }
            });

            return statusCode.intValue();
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
    protected int doUpdate(final Transaction transaction) {
        try {
            Long statusCode = RedisHelper.execute(jedisPool, new JedisCallback<Long>() {

                @Override
                public Long doInJedis(Jedis jedis) {
                    //设置事务最后更新时间
                    transaction.updateTime();
                    //设置事务最新版本号
                    transaction.updateVersion();

                    List<byte[]> params = new ArrayList<byte[]>();

                    for (Map.Entry<byte[], byte[]> entry : ExpandTransactionSerializer.serialize(serializer, transaction).entrySet()) {
                        params.add(entry.getKey());
                        params.add(entry.getValue());
                    }

                    //根据Key前缀、事务编号获取Redis Key,使用 Redis HSETNX更新事务,不存在当前版本的值则进行设置,实现类似乐观锁的更新
                    Object result = jedis.eval(String.format("if redis.call('hget',KEYS[1],'VERSION') == '%s' then redis.call('hmset', KEYS[1], unpack(ARGV)); return 1; end; return 0;",
                            transaction.getVersion() - 1).getBytes(),
                            Arrays.asList(RedisHelper.getRedisKey(keyPrefix, transaction.getXid())), params);

                    return (Long) result;
                }
            });

            return statusCode.intValue();
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
    protected int doDelete(final Transaction transaction) {
        try {
            Long result = RedisHelper.execute(jedisPool, new JedisCallback<Long>() {
                @Override
                public Long doInJedis(Jedis jedis) {
                    //根据Key前缀、事务编号获取Redis Key,按照Redis Key删除事务
                    return jedis.del(RedisHelper.getRedisKey(keyPrefix, transaction.getXid()));
                }
            });

            return result.intValue();
        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
    }

    /**
     * 根据事务编号查询事务
     *
     * @param xid
     * @return
     */
    @Override
    protected Transaction doFindOne(final Xid xid) {
        try {
            Long startTime = System.currentTimeMillis();

            Map<byte[], byte[]> content = RedisHelper.execute(jedisPool, new JedisCallback<Map<byte[], byte[]>>() {
                @Override
                public Map<byte[], byte[]> doInJedis(Jedis jedis) {
                    //根据Key前缀、事务编号获取Redis Key,使用 Redis HGETALL获取事务
                    return jedis.hgetAll(RedisHelper.getRedisKey(keyPrefix, xid));
                }
            });
            logger.info("redis find cost time :" + (System.currentTimeMillis() - startTime));

            if (content != null && content.size() > 0) {
                return ExpandTransactionSerializer.deserialize(serializer, content);
            }
            return null;
        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
    }

    /**
     * 获取超过指定时间的事务集合
     *
     * @param date
     * @return
     */
    @Override
    protected List<Transaction> doFindAllUnmodifiedSince(Date date) {
        //获取Redis存储的所有事务
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
     * 获取Redis存储的所有事务
     *
     * @return
     */
    protected List<Transaction> doFindAll() {
        try {
            final Set<byte[]> keys = RedisHelper.execute(jedisPool, new JedisCallback<Set<byte[]>>() {

                @Override
                public Set<byte[]> doInJedis(Jedis jedis) {
                    return jedis.keys((keyPrefix + "*").getBytes());
                }
            });

            return RedisHelper.execute(jedisPool, new JedisCallback<List<Transaction>>() {
                @Override
                public List<Transaction> doInJedis(Jedis jedis) {
                    Pipeline pipeline = jedis.pipelined();
                    for (final byte[] key : keys) {
                        pipeline.hgetAll(key);
                    }

                    List<Object> result = pipeline.syncAndReturnAll();
                    List<Transaction> list = new ArrayList<Transaction>();
                    for (Object data : result) {
                        if (data != null && ((Map<byte[], byte[]>) data).size() > 0) {
                            list.add(ExpandTransactionSerializer.deserialize(serializer, (Map<byte[], byte[]>) data));
                        }
                    }

                    return list;
                }
            });

        } catch (Exception e) {
            throw new TransactionIOException(e);
        }
    }
}