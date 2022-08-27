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
    
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("异步超时:\n" +
                               " 其他原因导致的异步超时\n" +
                               " \n" +
                               " 1. 用到的所有集群全部正常开启\n" +
                               "    hadoop hbase redis zk kafka maxwell\n" +
                               "    预处理app 详情app  dimapp\n" +
                               " 2. 检查下redis\n" +
                               "    启动redis一定要正确的配置\n" +
                               "        redis-server /etc/redis.conf\n" +
                               " \n" +
                               " 3. 检查下6张维度表是否齐全, 并且都有数据\n" +
                               "     maxwell 同步下所有的维度数据\n" +
                               "     \n" +
                               " 4. 调试\n" +
                               "        id传错了\n" +
                               "        \n" +
                               " 5. 找我");
        super.timeout(input, resultFuture);
        
    }
}
