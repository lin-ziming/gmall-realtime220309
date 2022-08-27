package com.atguigu.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author lzc
 * @Date 2022/8/27 10:29
 */
public class RedisUtil {
    
    
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(10);
        config.setMaxWaitMillis(10 * 1000);
        config.setMinIdle(2);
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        
        
        pool = new JedisPool(config, "hadoop162", 6379);
        
    }
    
    private static JedisPool pool;
    
    public static Jedis getRedisClient() {
        return pool.getResource();
    }
}
