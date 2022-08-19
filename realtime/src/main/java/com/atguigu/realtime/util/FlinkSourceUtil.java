package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
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
        
        
        return new FlinkKafkaConsumer<String>(
            topic,
            new KafkaDeserializationSchema<String>() {
                // 反序列化之后的数据类型
                @Override
                public TypeInformation<String> getProducedType() {
                    return Types.STRING;
//                    return TypeInformation.of(new TypeHint<String>() {});
                }
                
                // 要不要结束流
                @Override
                public boolean isEndOfStream(String nextElement) {
                    return false;
                }
                
                @Override
                public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                    byte[] value = record.value();// 获取kakfa中读取的valjue
                    if (value != null) {
                        return new String(value, StandardCharsets.UTF_8);
                    }
                    return null;
                }
            }, props
        );
    }
}
