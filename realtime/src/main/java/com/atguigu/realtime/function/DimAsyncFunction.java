package com.atguigu.realtime.function;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.util.DimUtil;
import com.atguigu.realtime.util.DruidDSUtil;
import com.atguigu.realtime.util.RedisUtil;
import com.atguigu.realtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author lzc
 * @Date 2022/8/27 14:19
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    
    private ThreadPoolExecutor threadPool;
    private DruidDataSource druidDataSource;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getThreadPool();
        druidDataSource = DruidDSUtil.getDruidDataSource();
        
    }
    
    public abstract String getTable();
    public abstract String getId(T input);
    public abstract void addDim(JSONObject dim, T input);
    
    @Override
    public void asyncInvoke(T input,
                            ResultFuture<T> resultFuture) throws Exception {
        // 多线程+多客户端
        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                DruidPooledConnection phoenixConn = null;
                Jedis redisClient = null;
                try {
    
                    phoenixConn = druidDataSource.getConnection();
                    redisClient = RedisUtil.getRedisClient();
    
                    JSONObject dim = DimUtil.readDim(redisClient, phoenixConn, getTable(), getId(input));
                    addDim(dim, input);
                    resultFuture.complete(Collections.singletonList(input)); // 把元素放入后序的流中
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    if (phoenixConn != null) {
                        try {
                            phoenixConn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
    
                    if (redisClient != null) {
                        redisClient.close();
                    }
                }
    
            }
        });
    }
}
