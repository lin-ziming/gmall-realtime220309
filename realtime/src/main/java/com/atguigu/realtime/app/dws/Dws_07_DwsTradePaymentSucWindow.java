package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TradePaymentWindowBean;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/8/26 11:25
 */
public class Dws_07_DwsTradePaymentSucWindow extends BaseAppV1 {
    public static void main(String[] args) {
        // 需求启动的app: 1. 预处理  2. 下单详情  3. 支付成功详情
        
        new Dws_07_DwsTradePaymentSucWindow().init(
            4007,
            2,
            "Dws_07_DwsTradePaymentSucWindow",
            Constant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        stream
            .map(JSON::parseObject)
            .keyBy(obj -> obj.getString("user_id"))
            .process(new KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>() {
                
                private ValueState<String> lastPaySucState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    lastPaySucState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastPaySucState", String.class));
                }
                
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<TradePaymentWindowBean> out) throws Exception {
                    String lastPaySucDate = lastPaySucState.value();
                    long ts = obj.getLong("ts") * 1000;
                    String today = AtguiguUtil.toDate(ts);
                    
                    Long paymentSucUniqueUserCount = 0L;
                    
                    // 支付成功新用户数
                    Long paymentSucNewUserCount = 0L;
                    
                    if (!today.equals(lastPaySucDate)) {
                        // 这个用户的今天第一次支付成功
                        paymentSucUniqueUserCount = 1L;
                        lastPaySucState.update(today);
                        
                        // 是否首次支付(是否是新支付用户)
                        if (lastPaySucDate == null) {
                            paymentSucNewUserCount = 1L;
                        }
                        
                    }
                    
                    if (paymentSucUniqueUserCount == 1) {
                        out.collect(new TradePaymentWindowBean("", "",
                                                               "",
                                                               paymentSucUniqueUserCount, paymentSucNewUserCount,
                                                               ts
                        ));
                    }
                    
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradePaymentWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean bean1,
                                                         TradePaymentWindowBean bean2) throws Exception {
                        bean1.setPaymentSucUniqueUserCount(bean1.getPaymentSucUniqueUserCount() + bean2.getPaymentSucUniqueUserCount());
                        bean1.setPaymentNewUserCount(bean1.getPaymentNewUserCount() + bean2.getPaymentNewUserCount());
                        
                        return bean1;
                    }
                },
                new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window,
                                      Iterable<TradePaymentWindowBean> values,
                                      Collector<TradePaymentWindowBean> out) throws Exception {
                        TradePaymentWindowBean bean = values.iterator().next();
                        
                        bean.setStt(AtguiguUtil.toDateTime(window.getStart()));
                        bean.setEdt(AtguiguUtil.toDateTime(window.getEnd()));
                        
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
            .addSink(FlinkSinkUtil.getDorisSink("gmall2022.dws_trade_payment_suc_window"));
    }
}
/*
{
    "order_detail_id":"10319",
    "order_id":"4267",
    "user_id":"1832",
    "sku_id":"11",
    "sku_name":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机",
    "province_id":"17",
    "activity_id":"2",
    "activity_rule_id":"4",
    "coupon_id":null,
    "payment_type_code":"1103",
    "payment_type_name":"银联",
    "callback_time":"2022-08-26 03:30:38",
    "source_id":null,
    "source_type_code":"2403",
    "source_type_name":"智能推荐",
    "sku_num":"3",
    "split_original_amount":"24591.0000",
    "split_activity_amount":"1200.0",
    "split_coupon_amount":null,
    "split_payment_amount":"23391.0",
    "ts":1661484619,
    "row_op_ts":"2022-08-26 03:30:24.307Z"
}


统计当日支付成功独立用户数和首次支付成功用户数。

日支付成功独立用户数:
    当日有多少用户支付成功
首次支付成功用户数:
    当日是用户在平台第一次支付成功

数据源:
    支付成功事务事实表

计算:
    使用状态
        存储用户最后一次成功支付的年月日

        这次支付成功的年月日和最后一次做对比

            如果不一样, 则进入第一次支付成功  ....

            如果最后一次的日期是null, 表示首次支付成功
 */
