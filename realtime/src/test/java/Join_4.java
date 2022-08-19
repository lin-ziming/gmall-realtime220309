import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/19 10:30
 */
public class Join_4 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 3000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 给join的时候的状态设置ttl
        /*tEnv.executeSql("create table s3(" +
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
                            ")");*/
    
        tEnv.executeSql("create table s3(" +
                            "id string, " +
                            "name string," +
                            " age int " +
                            ")with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers'='hadoop162:9092', " +
                            " 'properties.group.id'='abc', " +
                            " 'topic'='s3', " +
                            " 'format'='json' " +
                            ")");
    
    
        tEnv.sqlQuery("select * from s3").execute().print();
      
    }
}
/*

内连接: 只有新增
    可以用普通的kafka   建议用这个
    也可以用upsert kafka

左连接: 会有 更新
    只能用 upset kafka


 */
