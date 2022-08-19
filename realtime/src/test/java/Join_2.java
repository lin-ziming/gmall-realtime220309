import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/8/19 10:30
 */
public class Join_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 给join的时候的状态设置ttl
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        
        tEnv.executeSql("create table t1(id string, name string" +
                            ")with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers'='hadoop162:9092', " +
                            " 'properties.group.id'='atguigu', " +
                            " 'topic'='s1', " +
                            " 'format'='csv' " +
                            ")");
        
        tEnv.executeSql("create table t2(id string, age int" +
                            ")with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers'='hadoop162:9092', " +
                            " 'properties.group.id'='atguigu', " +
                            " 'topic'='s2', " +
                            " 'format'='csv' " +
                            ")");
        
        
        tEnv.sqlQuery("select " +
                          " t1.id, " +
                          " name, " +
                          " age " +
                          "from t1 " +
                          "left join t2 on t1.id=t2.id")
            .execute()
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
