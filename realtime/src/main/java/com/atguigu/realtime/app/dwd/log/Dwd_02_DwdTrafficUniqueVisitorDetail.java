package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/17 14:52
 */
public class Dwd_02_DwdTrafficUniqueVisitorDetail extends BaseAppV1 {
    
    public static void main(String[] args) {
        new Dwd_02_DwdTrafficUniqueVisitorDetail().init(
            3002,
            2,
            "Dwd_02_DwdTrafficUniqueVisitorDetail",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
        
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // ctrl+alt+l
        stream
            .map(JSON::parseObject)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
            )
            .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {
                
                private ValueState<String> visitDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    visitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visitDateState", String.class));
                }
                
                @Override
                public void process(String uid,
                                    Context ctx,
                                    Iterable<JSONObject> elements,
                                    Collector<String> out) throws Exception {
                    // ??????????????????????????????
                    String date = visitDateState.value();
                    String today = AtguiguUtil.toDate(ctx.window().getStart());
                    
                    if (!today.equals(date)) {  // ??????????????????????????????, ?????????????????????????????????
                        List<JSONObject> list = AtguiguUtil.toList(elements);
                        
                        //                        JSONObject min = Collections.min(list, (o1, o2) -> o1.getLong("ts").compareTo(o2.getLong("ts")));
                        JSONObject min = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
                        out.collect(min.toJSONString());
                        
                        visitDateState.update(today);// ????????????
                    }
                    
                    
                }
            })
            .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_UV_DETAIL));
    }
}
/*
DAU

dwd??????

???????????????????????????????????????????????????

?????????:
    ????????????
        ??????, ??????, ?????????????????????

        ??????app???

    ??????
        ?????????????????????????????????


??????????????????????????????????
    ????????????

    ??????????????????, ???????????????????????????, ????????????????????????????????????
 */