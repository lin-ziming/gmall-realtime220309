package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/8/15 14:42
 */
public class FlinkSourceUtil {
    public static SourceFunction<String> getKafkafSource(String groupId, String topic) {
    
    
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        
        props.setProperty("isolation.level", "read_committed");
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "latest");
        
        
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }
}
