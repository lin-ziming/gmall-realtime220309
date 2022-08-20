import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/8/19 10:30
 */
public class Join_7 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        tEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(8));
        tEnv.getConfig().getConfiguration().setString("table.local-time-zone", "UTC");
        
        // 给join的时候的状态设置ttl
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        
        tEnv.executeSql("create table t1(" +
                            "id string, " +
                            "name string, " +
                            "crt as CONVERT_TZ(date_format(current_row_timestamp(), 'yyyy-MM-dd HH:mm:ss'), 'UTC', 'Asia/Shanghai'), " +
                            "a as date_format(current_row_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')" +
                            ")with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers'='hadoop162:9092', " +
                            " 'properties.group.id'='atguigu', " +
                            " 'topic'='s1', " +
                            " 'format'='csv' " +
                            ")");
    
        Table t1 = tEnv.from("t1");
    
        //        tEnv.sqlQuery("select * from t1").execute().print();
        tEnv.toAppendStream(t1, Row.class).print();
        
        env.execute();
        
       
    }
}
