package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/8/19 9:16
 */
public class Dwd_03_DwdTrafficUserJumpDetail extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_03_DwdTrafficUserJumpDetail().init(
            3003,
            2,
            "Dwd_03_DwdTrafficUserJumpDetail",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":18000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":30000} "
            );
        
        KeyedStream<JSONObject, String> keyedStream = stream
            .map(JSON::parseObject)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
            )
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"));
        
        // 1. 定义模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
            .<JSONObject>begin("entry")
            .where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    // last_page_id = null
                    String lastPageId = value.getJSONObject("page").getString("last_page_id");
                    return lastPageId == null || lastPageId.length() == 0;
                }
            })
            .next("second")
            .where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    String lastPageId = value.getJSONObject("page").getString("last_page_id");
                    return lastPageId != null && lastPageId.length() > 0;
                }
            })
            .within(Time.seconds(5));
        // 2. 把模式作用到流上
        PatternStream<JSONObject> ps = CEP.pattern(keyedStream, pattern);
    
        // 3. 从模式流中取出想要的数据
        SingleOutputStreamOperator<JSONObject> normal = ps.select(
            new OutputTag<JSONObject>("timeout") {},
            new PatternTimeoutFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject timeout(Map<String, List<JSONObject>> map,
                                          long timeoutTimestamp) throws Exception {
                    return map.get("entry").get(0);
                }
            },
            new PatternSelectFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                    return null;
                }
            }
        );
    
        normal.getSideOutput(new OutputTag<JSONObject>("timeout") {}).print();
    
    }
}
