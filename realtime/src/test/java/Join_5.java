import com.atguigu.realtime.app.BaseAppV1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/19 10:30
 */
public class Join_5 extends BaseAppV1 {
    
    public static void main(String[] args) {
        new Join_5().init(
            10000,
            2,
            "Join_5",
            "s3"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }
}
/*

内连接: 只有新增
    可以用普通的kafka   建议用这个
    也可以用upsert kafka

左连接: 会有 更新
    只能用 upset kafka


 */
