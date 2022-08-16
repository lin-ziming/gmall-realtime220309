package com.atguigu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;

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
        // 2. 读取配置信息
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        // 3. 数据流和广播流做connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataTpStream = connect(etledStream, tpStream);
        dataTpStream.print();
        // 4. 根据不同的配置信息, 把不同的维度写入到不同的Phoenix的表中
        
    }
    
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<JSONObject> dataStream,
                                                                                 SingleOutputStreamOperator<TableProcess> tpStream) {
        // 0. 根据配置信息,在Phoenix中创建相应的维度表
        tpStream = tpStream.map(new RichMapFunction<TableProcess, TableProcess>() {
            
            private Connection conn;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                conn = JdbcUtil.getPhoenixConnection();
                
            }
            
            @Override
            public void close() throws Exception {
                JdbcUtil.closeConnection(conn);
            }
            
            @Override
            public TableProcess map(TableProcess tp) throws Exception {
                // 建表操作
                // create table if not exists table(name varchar, age varchar, constraint abc primary key(name)) null
                
                // 1. 拼接一个sql语句
                StringBuilder sql = new StringBuilder();
                sql
                    .append("create table if not exists ")
                    .append(tp.getSinkTable())
                    .append("(")
                    .append(tp.getSinkColumns().replaceAll("[^,]+", "$0 varchar"))
                    .append(", constraint pk primary key(")
                    .append(tp.getSinkPk() == null ? "id" : tp.getSinkPk())
                    .append("))")
                    .append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend());
                
                System.out.println("phoenix建表语句: " + sql);
                // 2. 获取预处理语句
                PreparedStatement ps = conn.prepareStatement(sql.toString());
                
                // 3. 给sql中的占位符赋值(查增删改), ddl: 建表语句一般不会有占位符
                // 略
                // 4. 执行
                ps.execute();
                // 5. 关闭 ps
                ps.close();
                return tp;
            }
        });
        
        
        // 1. 把配置流做成广播流
        // key: source_table
        // value: TableProcess
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBcStream = tpStream.broadcast(tpStateDesc);
        // 2. 让数据流去connect 广播流
        return dataStream
            .connect(tpBcStream)
            .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                // 处理数据流中的元素
                @Override
                public void processElement(JSONObject value,
                                           ReadOnlyContext ctx,
                                           Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    // 4. 处理数据流中数据的时候, 从广播状态读取他对应的配置信息
                    // 根据什么获取到配置信息: 根据mysql中的表名
                    ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    String key = value.getString("table");
                    
                    TableProcess tp = state.get(key);
                    // 如果不是维度表或者不需要sink的维度表, tp应该是null
                    if (tp != null) {
                        // 数据中的元数据信息无用了, 可以只取数据信息
                        JSONObject data = value.getJSONObject("data");
                        // 操作类型写入到data, 后面有用
                        data.put("op_type", value.getString("type"));
                        out.collect(Tuple2.of(data, tp));
                    }
                    
                }
                
                // 处理广播流中的元素
                @Override
                public void processBroadcastElement(TableProcess value,
                                                    Context ctx,
                                                    Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    // 3. 把配置信息写入到广播部状态
                    String key = value.getSourceTable();
                    BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    state.put(key, value);
                    
                    // 在Phoenix建表. 建表语句应该在广播之前, 否则会出现多次建表的情况
                    //checkTable(value);
                }
                
                //                private void checkTable(TableProcess value) {
                //                    System.out.println(value.getSourceTable() + "  " + value);
                //                     建表多次  create table if not exists a();
                //                }
            });
        
        
    }
    
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("hadoop162")
            .port(3306)
            .databaseList("gmall_config") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
            .tableList("gmall_config.table_process") // set captured table
            .username("root")
            .password("aaaaaa")
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();
        
        return env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
            .map(json -> {
                JSONObject obj = JSON.parseObject(json);
                return obj.getObject("after", TableProcess.class);
            });
        
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
/*
https://developer.aliyun.com/article/777502
https://github.com/ververica/flink-cdc-connectors


 SALT_BUCKETS = 4
 盐表
 
 ------------
 regionserver
 region
  数据
  
默认情况 建表一张表只有一个region

当region膨胀一定程度, 会自动分裂
    
    旧: 10G 一分为2
    
    新: ...
    
    hadoop162
     r1  r2
     
   自动迁移
    r2 迁移到163
    
 -----
 避免分裂和迁移: 预分区
 
 
 --------
 
 Phoenix建表: 如何创建带有预分区的表?
 
 
 
 
 
 
 
 */