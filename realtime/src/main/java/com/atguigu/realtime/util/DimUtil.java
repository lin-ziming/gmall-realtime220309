package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/27 8:53
 */
public class DimUtil {
    
    public static JSONObject readDimFromPhoenix(Connection phoenixConn, String table, String id) {
        String sql = "select * from " + table + " where id=?";
        
        String[] args = {id};
        
        // 一个通用的jdbc查询方法, 返回值有可能多行, 所以返回List集合
        List<JSONObject> list = JdbcUtil.queryList(phoenixConn, sql, args, JSONObject.class);
        
        // 对当前这个sql语句, 一定只有一行, 直接get(0)
        return list.get(0);
    }
    
    public static JSONObject readDim(Jedis redisClient,
                                     Connection phoenixConn,
                                     String table,
                                     String id) {
        // 1. 从redis读取维度信息
        JSONObject dim = DimUtil.readDimFromRedis(redisClient, table, id);
        // 2. 如果存在, 则直接返回
        if (dim == null) {
            System.out.println("走数据库: " + table + "  " + id);
            // 没有读到, 从phoenix读
            dim = readDimFromPhoenix(phoenixConn, table, id);
            // 维度写入读到redis
            writeDimToRedis(redisClient, table, id, dim);
            
        } else {
            System.out.println("走缓存: " + table + "  " + id);
        }
        // 3. 如果缓存中不存在, 去phoenix中读, 返回, 把维度写入到redis中
        return dim;
    }
    
    //TODO
    private static void writeDimToRedis(Jedis redisClient, String table, String id, JSONObject dim) {
    
    }
    
    //TODO
    private static JSONObject readDimFromRedis(Jedis redisClient, String table, String id) {
        return null;
    }
}
