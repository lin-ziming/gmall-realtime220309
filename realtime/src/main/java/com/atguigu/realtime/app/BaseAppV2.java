package com.atguigu.realtime.app;

import com.atguigu.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

/**
 * @Author lzc
 * @Date 2022/8/15 15:06
 */
public abstract class BaseAppV2 {
    protected abstract void handle(StreamExecutionEnvironment env,
                                   HashMap<String, DataStreamSource<String>> stream);
    
    public void init(int port, int p, String ckPathGroupIdJobName, String ... topics){
        if (topics.length == 0) {
            throw new RuntimeException("传入的topic的个数应该大于0, 至少应该传入一个topic...");
        }
        
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);
    
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/" + ckPathGroupIdJobName);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(20 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    
    
        HashMap<String, DataStreamSource<String>> streams = new HashMap<>();
        for (String topic : topics) {
            DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkafSource(ckPathGroupIdJobName, topic));
            streams.put(topic, stream);
        }
        
        //
        handle(env, streams);
    
        try {
            env.execute(ckPathGroupIdJobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
