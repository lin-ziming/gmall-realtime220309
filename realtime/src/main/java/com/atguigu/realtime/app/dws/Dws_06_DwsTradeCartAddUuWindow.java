package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.CartAddUuBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * @Date 2022/8/26 10:37
 */
public class Dws_06_DwsTradeCartAddUuWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_06_DwsTradeCartAddUuWindow().init(
            4006,
            2,
            "Dws_06_DwsTradeCartAddUuWindow",
            Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        stream
            .map(JSON::parseObject)
            // 找到某个用户的第一条加购记录
            .keyBy(obj -> obj.getString("user_id"))
            .process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {
    
                private ValueState<String> lastAddCartDateState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    lastAddCartDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAddCartDateState", String.class));
                }
    
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<CartAddUuBean> out) throws Exception {
                    // 业务数据中的ts是 秒, 所以要 * 1000
    
                    Long ts = obj.getLong("ts") * 1000;
                    String today = AtguiguUtil.toDate(ts);
    
                    if (!today.equals(lastAddCartDateState.value())) {
                        // 这个用户的今天第一次加购
                        lastAddCartDateState.update(today);
                        
                        out.collect(new CartAddUuBean("", "",
                                                      "",
                                                      1L,
                                                      ts));
                    }
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<CartAddUuBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean bean1,
                                                CartAddUuBean bean2) throws Exception {
                        bean1.setCartAddUuCt(bean1.getCartAddUuCt() + bean2.getCartAddUuCt());
                        return bean1;
                    }
                },
                new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void process(Context ctx,
                                        Iterable<CartAddUuBean> elements,
                                        Collector<CartAddUuBean> out) throws Exception {
                        CartAddUuBean bean = elements.iterator().next();
                        
                        bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
    
                        bean.setCurDate(AtguiguUtil.toDate(System.currentTimeMillis()));
    
    
                        out.collect(bean);
                    }
                }
            )
            .map(bean -> {
    
                SerializeConfig conf = new SerializeConfig();
                conf.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
                return JSON.toJSONString(bean, conf);
                
            })
            .addSink(FlinkSinkUtil.getDorisSink("gmall2022.dws_trade_cart_add_uu_window"));
    }
}
