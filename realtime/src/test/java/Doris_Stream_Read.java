import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/8/24 10:38
 */
public class Doris_Stream_Read {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        Properties props = new Properties();
        props.setProperty("fenodes", "hadoop162:7030");
        props.setProperty("username", "root");
        props.setProperty("password", "aaaaaa");
        props.setProperty("table.identifier", "test_db.table1");
    
        env
            .addSource(new DorisSourceFunction(new DorisStreamOptions(props), new SimpleListDeserializationSchema()))
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
       
    }
}
