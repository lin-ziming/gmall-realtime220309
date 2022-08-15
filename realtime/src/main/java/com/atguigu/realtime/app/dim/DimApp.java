package com.atguigu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/15 14:36
 */
public class DimApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DimApp().init(2001, 2, "DimApp", Constant.TOPIC_ODS_DB);
        
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 对流做操作
        // 1. 对业务数据做过滤 ETL
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        etledStream.print();
    
        // 2. 读取配置信息
        
        // 3. 数据流和广播流做connect
        
        // 4. 根据不同的配置信息, 把不同的维度写入到不同的Phoenix的表中
        
    }
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
      return stream
            .filter(json -> {
                try {
                    
                    JSONObject obj = JSON.parseObject(json.replaceAll("bootstrap-", ""));
                    
                    return "gmall2022".equals(obj.getString("database"))
                        && (
                        "insert".equals(obj.getString("type"))
                            || "update".equals(obj.getString("type")))
                        && obj.getString("data") != null
                        && obj.getString("data").length() > 2;
                    
                    
                } catch (Exception e) {
                    System.out.println("json 格式有误, 你的数据是: " + json);
                    return false;
                }
            })
          .map(JSON::parseObject);  // 转成jsonObject,方便后序使用
        
    }
}
