package com.atguigu.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.DruidDSUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;

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
        // upsert into user(aa,b,c)values(?,?,?)
        StringBuilder sql = new StringBuilder();
        sql
            .append("upsert into ")
            .append(tp.getSinkTable())
            .append("(")
            .append(tp.getSinkColumns())
            .append(")values(")
            .append(tp.getSinkColumns().replaceAll("[^,]+", "?"))
            .append(")");
        System.out.println("插入语句: " + sql);
        // 2. 使用连接对象获取一个 PrepareStatement
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        // 3. 给占位符赋值 TODO
        String[] cs = tp.getSinkColumns().split(",");
        for (int i = 0, count = cs.length; i < count; i++) {
            Object v = data.get(cs[i]);  // null + ""="null"
            String vv = v == null ? null : v.toString();
            ps.setString(i + 1, vv);
        }
        
        // 4. 执行sql
        ps.execute();
        conn.commit();
        // 5.关闭PrepareStatement
        ps.close();
        // 6. 归还连接
        conn.close();
        
    }
}
