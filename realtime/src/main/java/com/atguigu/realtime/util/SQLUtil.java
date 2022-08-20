package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;

/**
 * @Author lzc
 * @Date 2022/8/19 15:08
 */
public class SQLUtil {
    public static String getKafkaSource(String topic, String groupId) {
        return "with(" +
            " 'connector'='kafka', " +
            " 'properties.bootstrap.servers'='" + Constant.KAFKA_BROKERS + "', " +
            " 'properties.group.id'='" + groupId + "', " +
            " 'topic'='" + topic + "', " +
            " 'format'='json', " +
            " 'scan.startup.mode'='latest-offset' " +
            ")";
    }
    
    public static String getKafkaSink(String topic) {
        return "with(" +
            " 'connector'='kafka', " +
            " 'properties.bootstrap.servers'='" + Constant.KAFKA_BROKERS + "', " +
            " 'topic'='" + topic + "', " +
            " 'format'='json' " +
            ")";
    }
    
    public static String getUpsertKafkaSink(String topic) {
        return "with(" +
            " 'connector'='upsert-kafka', " +
            " 'properties.bootstrap.servers'='" + Constant.KAFKA_BROKERS + "', " +
            " 'topic'='" + topic + "', " +
            " 'key.format'='json', " +
            " 'value.format'='json' " +
            ")";
    }
}
