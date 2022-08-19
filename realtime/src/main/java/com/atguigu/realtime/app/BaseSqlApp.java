package com.atguigu.realtime.app;

import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/15 15:06
 */
public abstract class BaseSqlApp {
    
    
    public void init(int port, int p, String ckPathAndJobId) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);
        
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/" + ckPathAndJobId);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(20 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 给sql job起名字
        tEnv.getConfig().getConfiguration().setString("pipeline.name", ckPathAndJobId);
        
        handle(env, tEnv);
        
        
    }
    
    protected abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv);
    
    // 读取ods_db数据
    public void readOdsDb(StreamTableEnvironment tEnv, String groupId) {
        tEnv.executeSql("create table ods_db(" +
                            " `database` string, " +
                            " `table` string, " +
                            " `type` string, " +
                            " `ts` bigint, " +
                            " `data` map<string, string>, " +
                            " `old` map<string, string>, " +
                            " `pt` as proctime() " +  // lookup join的时候要用
                            ")" + SQLUtil.getKafkaSource(Constant.TOPIC_ODS_DB, groupId));
    }
    
    public void readBaseDic(StreamTableEnvironment tEnv) {
        tEnv.executeSql("create table base_dic(" +
                            "dic_code string, " +
                            "dic_name string " +
                            ")with(" +
                            " 'connector'='jdbc',  " +
                            " 'url' = 'jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false'," +
                            " 'table-name' = 'base_dic', " +
                            " 'username' = 'root',  " +
                            " 'password' = 'aaaaaa'," +
                            " 'lookup.cache.max-rows' = '10', " +
                            " 'lookup.cache.ttl' = '1 hour' " +
                            ")");
    }
    
}
