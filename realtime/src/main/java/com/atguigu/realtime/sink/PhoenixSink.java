package com.atguigu.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.DruidDSUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @Author lzc
 * @Date 2022/8/16 14:35
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    
    private DruidDataSource druidDataSource;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取连接池
        druidDataSource = DruidDSUtil.getDruidDataSource();
        
        
        
    }
    
    @Override
    public void close() throws Exception {
        druidDataSource.close();  // app关闭之前关闭连接池
    }
    
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        // 每来一条数据, 从连接池获取一个可用的连接, 这样可以避免长连接被服务器自动关闭的问题.
    
        DruidPooledConnection conn = druidDataSource.getConnection();
    
        JSONObject data = value.f0;
        TableProcess tp = value.f1;
        
        // 1. 拼接sql语句. 一定有占位符
        
        StringBuilder sql = new StringBuilder();
        
        // 2. 使用连接对象获取一个 PrepareStatement
        
        // 3. 给占位符赋值
        
        
        // 4. 执行sql
        
        // 5.关闭PrepareStatement
        
        // 6. 归还连接
    
    }
}
