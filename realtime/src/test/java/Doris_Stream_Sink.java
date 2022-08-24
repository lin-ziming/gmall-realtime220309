import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/8/24 10:38
 */
public class Doris_Stream_Sink {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // doris 流中只值两种数据sink: 1. json字符串 2. RowData
        // 1. json字符串
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("strip_outer_array", "true");
        env
            .fromElements(
                "{\"siteid\": \"10\", \"citycode\": \"1001\", \"username\": \"ww\",\"pv\": \"100\"}"
            )
            .addSink(DorisSink
                         .sink(
                             new DorisExecutionOptions.Builder()
                                 .setBatchIntervalMs(2000L)
                                 .setBatchSize(1024 * 1024)
                                 .setEnableDelete(false)
                                 .setMaxRetries(3)
                                 .setStreamLoadProp(props)
                                 .build(),
                             new DorisOptions.Builder()
                                 .setFenodes("hadoop162:7030")
                                 .setUsername("root")
                                 .setPassword("aaaaaa")
                                 .setTableIdentifier("test_db.table1")
                                 .build()
                         )
            );
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
