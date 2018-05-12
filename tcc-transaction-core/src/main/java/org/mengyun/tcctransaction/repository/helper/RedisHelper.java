package org.mengyun.tcctransaction.repository.helper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.transaction.xa.Xid;

/**
 * Redis工具类
 */
public class RedisHelper {

    /**
     * 根据Key前缀、事务编号获取Redis Key
     *
     * @param keyPrefix
     * @param xid
     * @return
     */
    public static byte[] getRedisKey(String keyPrefix, Xid xid) {
        return new StringBuilder().append(keyPrefix).append(xid.toString()).toString().getBytes();
    }

    /**
     * 根据Key前缀、全局事务编号以及分支事务编号获取Redis Key
     *
     * @param keyPrefix
     * @param globalTransactionId
     * @param branchQualifier
     * @return
     */
    public static byte[] getRedisKey(String keyPrefix, String globalTransactionId, String branchQualifier) {
        return new StringBuilder().append(keyPrefix).append(globalTransactionId).append(":").append(branchQualifier).toString().getBytes();
    }

    public static byte[] getVersionKey(String keyPrefix, Xid xid) {
        return new StringBuilder().append("VER:").append(keyPrefix).append(xid.toString()).toString().getBytes();
    }

    public static byte[] getVersionKey(String keyPrefix, String globalTransactionId, String branchQualifier) {
        return new StringBuilder().append("VER:").append(keyPrefix).append(globalTransactionId).append(":").append(branchQualifier).toString().getBytes();
    }

    /**
     * 通过Jedis池执行Jedis回调
     *
     * @param jedisPool
     * @param callback
     * @param <T>
     * @return
     */
    public static <T> T execute(JedisPool jedisPool, JedisCallback<T> callback) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return callback.doInJedis(jedis);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}