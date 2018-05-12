package org.mengyun.tcctransaction.repository.helper;

import redis.clients.jedis.Jedis;

/**
 * Jedis回调
 */
public interface JedisCallback<T> {

    public T doInJedis(Jedis jedis);
}