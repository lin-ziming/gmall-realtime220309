import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;

/**
 * @Author lzc
 * @Date 2022/8/24 10:38
 */
public class Doris_Stream_Sink_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // doris 流中只值两种数据sink: 1. json字符串 2. RowData
    
    
        LogicalType[] types = {new IntType(), new SmallIntType(), new VarCharType(), new BigIntType()};
        String[] fields = {"siteid", "citycode", "username", "pv"};
    
    
    
    
        env
            .fromElements(
                "{\"siteid\": \"11\", \"citycode\": \"1002\", \"username\": \"abc\",\"pv\": \"1000\"}"
            )
            .map(json -> {
                JSONObject obj = JSON.parseObject(json);
                
                GenericRowData rowData = new GenericRowData(4);
                rowData.setField(0, obj.getIntValue("siteid"));
                rowData.setField(1, obj.getShortValue("citycode"));
                rowData.setField(2, StringData.fromString(obj.getString("username")));
                rowData.setField(3, obj.getLong("pv"));
                return rowData;
                
            })
            .addSink(DorisSink
                         .sink(
                             fields,
                             types,
                             new DorisExecutionOptions.Builder()
                                 .setBatchIntervalMs(2000L)
                                 .setBatchSize(1024 * 1024)
                                 .setEnableDelete(false)
                                 .setMaxRetries(3)
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
