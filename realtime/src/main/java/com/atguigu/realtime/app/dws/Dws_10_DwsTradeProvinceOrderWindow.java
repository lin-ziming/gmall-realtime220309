package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TradeProvinceOrderWindow;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.function.DimAsyncFunction;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Author lzc
 * @Date 2022/8/27 15:46
 */
public class Dws_10_DwsTradeProvinceOrderWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_10_DwsTradeProvinceOrderWindow().init(
            4010,
            2,
            "Dws_10_DwsTradeProvinceOrderWindow",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 1. 按照 order_detail_id 去重
        SingleOutputStreamOperator<JSONObject> distinctedStream = distinctByOrderDetailId(stream);
        // 2. 把数据封装到pojo中
        SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStream = parseToPojo(distinctedStream);
        // 3. 按照 sku_id 分组 开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStreamWithoutDim = windowAndAgg(beanStream);
        // 4. 补充维度信息
        SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStreamWithDim = addDim(beanStreamWithoutDim);
        // 5. 写出到doris中
        writeToDoris(beanStreamWithDim);
        
    }
    
    private void writeToDoris(SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStreamWithDim) {
        beanStreamWithDim
            .map(bean -> {
                SerializeConfig conf = new SerializeConfig();
                conf.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
                return JSON.toJSONString(bean, conf);
            })
            .addSink(FlinkSinkUtil.getDorisSink("gmall2022.dws_trade_province_order_window"));
    }
    
    private SingleOutputStreamOperator<TradeProvinceOrderWindow> addDim(
        SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStreamWithoutDim) {
        
        return AsyncDataStream.unorderedWait(
            beanStreamWithoutDim,
            new DimAsyncFunction<TradeProvinceOrderWindow>() {
                @Override
                public String getTable() {
                    return "dim_base_province";
                }
                
                @Override
                public String getId(TradeProvinceOrderWindow input) {
                    return input.getProvinceId();
                }
                
                @Override
                public void addDim(JSONObject dim, TradeProvinceOrderWindow bean) {
                    bean.setProvinceName(dim.getString("NAME"));
                    
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
        
    }
    
    private SingleOutputStreamOperator<TradeProvinceOrderWindow> windowAndAgg(
        SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStream) {
        return beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .keyBy(TradeProvinceOrderWindow::getProvinceId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow bean1,
                                                           TradeProvinceOrderWindow bean2) throws Exception {
                        bean1.setOrderAmount(bean1.getOrderAmount().add(bean2.getOrderAmount()));
                        bean1.getOrderIdSet().addAll(bean2.getOrderIdSet());
                        return bean1;
                    }
                },
                new ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void process(String skuId,
                                        Context ctx,
                                        Iterable<TradeProvinceOrderWindow> elements,
                                        Collector<TradeProvinceOrderWindow> out) throws Exception {
                        TradeProvinceOrderWindow bean = elements.iterator().next();
                        
                        bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
                        
                        bean.setCurDate(AtguiguUtil.toDate(System.currentTimeMillis()));
                        
                        bean.setOrderCount((long) bean.getOrderIdSet().size());
                        
                        out.collect(bean);
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<TradeProvinceOrderWindow> parseToPojo(
        SingleOutputStreamOperator<JSONObject> distinctedStream) {
        
        
        return distinctedStream.map(new MapFunction<JSONObject, TradeProvinceOrderWindow>() {
            
            @Override
            public TradeProvinceOrderWindow map(JSONObject value) throws Exception {
                
                return TradeProvinceOrderWindow.builder()
                    .provinceId(value.getString("province_id"))
                    .orderIdSet(new HashSet<>(Collections.singleton(value.getString("order_id"))))
                    .orderAmount(value.getBigDecimal("split_total_amount"))
                    .ts(value.getLong("ts") * 1000)
                    .build();
                
            }
        });
    }
    
    
    private SingleOutputStreamOperator<JSONObject> distinctByOrderDetailId(DataStreamSource<String> stream) {
        
        return stream
            .map(JSON::parseObject)
            .keyBy(obj -> obj.getString("id"))
            .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                
                private ValueState<JSONObject> maxTsDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    maxTsDateState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("maxTsDateState", JSONObject.class));
                }
                
                @Override
                public void processElement(JSONObject value,
                                           Context ctx,
                                           Collector<JSONObject> out) throws Exception {
                    
                    // 定时器触发的时候, 把状态中的数据输出
                    
                    if (maxTsDateState.value() == null) {
                        // 第一条数据来的时候, 把数据存到状态, 注册5s后触发的定时器
                        maxTsDateState.update(value);
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                    } else {
                        // 如果不是第一条, 则与状态中的数据做对比
                        String last = maxTsDateState.value().getString("row_op_ts");
                        String current = value.getString("row_op_ts");
                        
                        // 判断current时间是否大于last, 返回true表示大
                        if (AtguiguUtil.isLarger(current, last)) {
                            maxTsDateState.update(value);
                        }
                    }
                }
                
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<JSONObject> out) throws Exception {
                    // 当定时器触发的时候, 执行这个方法
                    out.collect(maxTsDateState.value());
                    
                }
            });
    }
    
}
