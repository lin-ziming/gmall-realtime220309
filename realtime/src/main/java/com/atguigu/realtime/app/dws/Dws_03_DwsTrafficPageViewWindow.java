package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/8/25 11:03
 */
public class Dws_03_DwsTrafficPageViewWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_03_DwsTrafficPageViewWindow().init(
            4003,
            2,
            "Dws_03_DwsTrafficPageViewWindow",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 1. 先找到详情页和首页, 封装到bean
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = findUv(stream);
        // 2. 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream = windowAndAgg(beanStream);
        // 3. 写出
        writeToDoris(resultStream);
    }
    
    private void writeToDoris(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream) {
        resultStream
            .map(bean -> {
                SerializeConfig config = new SerializeConfig();
                config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;  // 转成json的时候, 属性名使用下划线
                return JSON.toJSONString(bean, config);
            })
            .addSink(FlinkSinkUtil.getDorisSink("gmall2022.dws_traffic_page_view_window"));
    }
    
    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndAgg(
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream) {
        return beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean bean1,
                                                                TrafficHomeDetailPageViewBean bean2) throws Exception {
                        bean1.setHomeUvCt(bean1.getHomeUvCt() + bean2.getHomeUvCt());
                        bean1.setGoodDetailUvCt(bean1.getGoodDetailUvCt() + bean2.getGoodDetailUvCt());
                        return bean1;
                    }
                },
                new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void process(Context ctx,
                                        Iterable<TrafficHomeDetailPageViewBean> elements,
                                        Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        
                        TrafficHomeDetailPageViewBean bean = elements.iterator().next();
                        
                        bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
                        
                        bean.setCurDate(AtguiguUtil.toDate(System.currentTimeMillis()));
                        
                        out.collect(bean);
                        
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> findUv(DataStreamSource<String> stream) {
        return stream
            .map(JSON::parseObject)
            .filter(obj -> {
                String pageId = obj.getJSONObject("page").getString("page_id");
                return "home".equals(pageId) || "good_detail".equals(pageId);
            })
            .keyBy(obj -> obj.getJSONObject("common").getString("uid")) // 统计home的独立访客数: 把同一用户的记录分到同一组
            .process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                
                private ValueState<String> homeState;
                private ValueState<String> goodDetailState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    homeState = getRuntimeContext().getState(new ValueStateDescriptor<String>("homeState", String.class));
                    goodDetailState = getRuntimeContext().getState(new ValueStateDescriptor<String>("goodDetailState", String.class));
                }
                
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                    String pageId = obj.getJSONObject("page").getString("page_id");
                    Long ts = obj.getLong("ts");
                    // 找到这个用户今天的第一个home页面记录
                    // 找到这个用户今天的第一个good_detail页面记录
                    
                    Long homeUvCt = 0L;
                    Long goodDetailUvCt = 0L;
                    
                    String today = AtguiguUtil.toDate(ts);
                    // 是home, 并且今天和状态中的年月日不等, 就是今天的第一个home
                    if ("home".equals(pageId) && !today.equals(homeState.value())) {
                        homeUvCt = 1L;
                        // 更新状态
                        homeState.update(today);
                    } else if ("good_detail".equals(pageId) && !today.equals(goodDetailState.value())) {
                        goodDetailUvCt = 1L;
                        goodDetailState.update(today);
                    }
                    
                    // homeUvCt 和 goodDetailUvCt至少有一个不为1
                    if (homeUvCt + goodDetailUvCt >= 1) {
                        out.collect(new TrafficHomeDetailPageViewBean("", "",
                                                                      "",
                                                                      homeUvCt, goodDetailUvCt,
                                                                      ts
                        ));
                    }
                    
                }
            });
        
    }
}
/*
从 Kafka 页面日志主题读取数据，统计当日的首页和商品详情页独立访客数。
1. 数据源
    页面日志
    
2. 过滤出所有的首页访问记录和详情页访问记录

3. 封装bean中
    如果首页  首页 1
    如果是详情页.. ...
4. 开窗聚和

5. 写出doris

 */