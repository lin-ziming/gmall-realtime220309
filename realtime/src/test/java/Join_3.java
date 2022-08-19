import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/8/19 10:30
 */
public class Join_3 {
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
    
    
        Table result = tEnv.sqlQuery("select " +
                                        " t1.id, " +
                                        " name, " +
                                        " age " +
                                        "from t1 " +
                                        " left join t2 on t1.id=t2.id");
    
    
    
    
        /*tEnv.executeSql("create table t12(id string, name string, age int" +
                            ")with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers'='hadoop162:9092', " +
                            " 'topic'='s12', " +
                            " 'format'='json' " +
                            ")");*/
    
        tEnv.executeSql("create table t12(" +
                            "id string, " +
                            "name string," +
                            " age int, " +
                            "primary key(id) not enforced" +
                            ")with(" +
                            " 'connector'='upsert-kafka', " +
                            " 'properties.bootstrap.servers'='hadoop162:9092', " +
                            " 'topic'='s3', " +
                            " 'key.format'='json', " +
                            " 'value.format'='json' " +
                            ")");
        
        result.executeInsert("t12");
    
    
      
    }
}
/*

内连接: 只有新增
    可以用普通的kafka   建议用这个
    也可以用upsert kafka

左连接: 会有 更新
    只能用 upset kafka


 */
