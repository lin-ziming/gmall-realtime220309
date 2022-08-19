import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/8/19 10:30
 */
public class Join_6 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 用到lookup join的是, 参数对两张表均无效
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        
        tEnv.executeSql("create table t1(" +
                            "id string, " +
                            "pt as proctime() " +
                            ")with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers'='hadoop162:9092', " +
                            " 'properties.group.id'='atguigu', " +
                            " 'topic'='s1', " +
                            " 'format'='csv' " +
                            ")");
        
        tEnv.executeSql("create table dic(" +
                            "dic_code string, " +
                            "dic_name string " +
                            ")with(" +
                            " 'connector'='jdbc',  " +
                            "'url' = 'jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false',\n" +
                            " 'table-name' = 'base_dic', " +
                            " 'username' = 'root',  " +
                            " 'password' = 'aaaaaa'," +
                            " 'lookup.cache.max-rows' = '10', " +
                            "  'lookup.cache.ttl' = '30 second' " +
                            ")");
        tEnv.sqlQuery("select " +
                          " id, " +
                          " d.dic_name " +
                          "from t1 " +
                          " join dic for system_time as of t1.pt as d " +
                          " on t1.id=d.dic_code " +
                          "")
            .execute()
            .print();
        
        
        
        
    }
}
