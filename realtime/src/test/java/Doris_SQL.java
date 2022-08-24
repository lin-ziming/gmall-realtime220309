import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/24 10:38
 */
public class Doris_SQL {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        tEnv.executeSql("create table site(" +
                            " siteid int, " +
                            " citycode smallint, " +
                            " username string, " +
                            " pv bigint " +
                            ")with(" +
                            "  'connector' = 'doris', " +
                            "  'fenodes' = 'hadoop162:7030', " +
                            "  'table.identifier' = 'test_db.table1', " +
                            "  'username' = 'root', " +
                            "  'password' = 'aaaaaa' " +
                            ")");
        
        //        /tEnv.sqlQuery("select * from site").execute().print();
        
        tEnv.executeSql("insert into site(siteid, username, pv) values(13, 'abc', 3000) ");
        
    }
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Site {
        private Integer siteid;
        private Short citycode;
        private String username;
        private Long pv;
    }
}
