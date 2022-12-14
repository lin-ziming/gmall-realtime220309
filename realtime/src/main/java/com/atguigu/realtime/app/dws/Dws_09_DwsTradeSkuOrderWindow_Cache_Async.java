package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TradeSkuOrderBean;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Author lzc
 * @Date 2022/8/26 13:36
 */
public class Dws_09_DwsTradeSkuOrderWindow_Cache_Async extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow_Cache_Async().init(
            4009,
            2,
            "Dws_09_DwsTradeSkuOrderWindow_Cache_Async",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        
        
        // 1. ?????? order_detail_id ??????
        SingleOutputStreamOperator<JSONObject> distinctedStream = distinctByOrderDetailId(stream);
        // 2. ??????????????????pojo???
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(distinctedStream);
        // 3. ?????? sku_id ?????? ????????????
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDim = windowAndAgg(beanStream);
        // 4. ??????????????????
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithDim = addDim(beanStreamWithoutDim);
        // 5. ?????????doris???
        writeToDoris(beanStreamWithDim);
        
    }
    
    private void writeToDoris(SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithDim) {
        beanStreamWithDim
            .map(bean -> {
                SerializeConfig conf = new SerializeConfig();
                conf.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
                return JSON.toJSONString(bean, conf);
            })
            .addSink(FlinkSinkUtil.getDorisSink("gmall2022.dws_trade_sku_order_window"));
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> addDim(
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDim) {
        
        // 1. sku
        SingleOutputStreamOperator<TradeSkuOrderBean> skuInfoStream = AsyncDataStream.unorderedWait(
            beanStreamWithoutDim,
            new DimAsyncFunction<TradeSkuOrderBean>() {
                @Override
                public String getTable() {
                    return "dim_sku_info";
                }
                
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getSkuId();
                }
                
                @Override
                public void addDim(JSONObject dim, TradeSkuOrderBean bean) {
                    bean.setSkuName(dim.getString("SKU_NAME"));
                    bean.setTrademarkId(dim.getString("TM_ID"));
                    bean.setSpuId(dim.getString("SPU_ID"));
                    bean.setCategory3Id(dim.getString("CATEGORY3_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
        
        SingleOutputStreamOperator<TradeSkuOrderBean> tmStream = AsyncDataStream.unorderedWait(
            skuInfoStream,
            new DimAsyncFunction<TradeSkuOrderBean>() {
                @Override
                public String getTable() {
                    return "dim_base_trademark";
                }
            
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getTrademarkId();
                }
            
                @Override
                public void addDim(JSONObject dim, TradeSkuOrderBean bean) {
                    bean.setTrademarkName(dim.getString("TM_NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
        
        SingleOutputStreamOperator<TradeSkuOrderBean> spuStream = AsyncDataStream.unorderedWait(
            tmStream,
            new DimAsyncFunction<TradeSkuOrderBean>() {
                @Override
                public String getTable() {
                    return "dim_spu_info";
                }
                
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getSpuId();
                }
                
                @Override
                public void addDim(JSONObject dim, TradeSkuOrderBean bean) {
                    bean.setSpuName(dim.getString("SPU_NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
        
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(
            spuStream,
            new DimAsyncFunction<TradeSkuOrderBean>() {
                @Override
                public String getTable() {
                    return "dim_base_category3";
                }
                
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getCategory3Id();
                }
                
                @Override
                public void addDim(JSONObject dim, TradeSkuOrderBean bean) {
                    bean.setCategory3Name(dim.getString("NAME"));
                    bean.setCategory2Id(dim.getString("CATEGORY2_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(
            c3Stream,
            new DimAsyncFunction<TradeSkuOrderBean>() {
                @Override
                public String getTable() {
                    return "dim_base_category2";
                }
                
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getCategory2Id();
                }
                
                @Override
                public void addDim(JSONObject dim, TradeSkuOrderBean bean) {
                    bean.setCategory2Name(dim.getString("NAME"));
                    bean.setCategory1Id(dim.getString("CATEGORY1_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
        return AsyncDataStream.unorderedWait(
            c2Stream,
            new DimAsyncFunction<TradeSkuOrderBean>() {
                @Override
                public String getTable() {
                    return "dim_base_category1";
                }
                
                @Override
                public String getId(TradeSkuOrderBean input) {
                    return input.getCategory1Id();
                }
                
                @Override
                public void addDim(JSONObject dim, TradeSkuOrderBean bean) {
                    bean.setCategory1Name(dim.getString("NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
        
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndAgg(
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {
        return beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .keyBy(TradeSkuOrderBean::getSkuId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean bean1,
                                                    TradeSkuOrderBean bean2) throws Exception {
                        //                        System.out.println("bean1: " + bean1 + ", bean2: " + bean2);
                        bean1.setOriginalAmount(bean1.getOriginalAmount().add(bean2.getOriginalAmount()));
                        bean1.setActivityAmount(bean1.getActivityAmount().add(bean2.getActivityAmount()));
                        bean1.setCouponAmount(bean1.getCouponAmount().add(bean2.getCouponAmount()));
                        bean1.setOrderAmount(bean1.getOrderAmount().add(bean2.getOrderAmount()));
                        return bean1;
                    }
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String skuId,
                                        Context ctx,
                                        Iterable<TradeSkuOrderBean> elements,
                                        Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean bean = elements.iterator().next();
                        
                        bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
                        
//                        bean.setCurDate(AtguiguUtil.toDate(System.currentTimeMillis()));
                        bean.setCurDate(AtguiguUtil.toDate(ctx.window().getStart()));  //
                        
                        out.collect(bean);
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(
        SingleOutputStreamOperator<JSONObject> distinctedStream) {
        return distinctedStream.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            
            
            @Override
            public TradeSkuOrderBean map(JSONObject value) throws Exception {
                 /*
                {
                    "id":"4992",
                    "order_id":"2060",
                    "user_id":"219",
                    "sku_id":"23",
                    "sku_name":"???????????? ??????????????? ???????????? 5kg",
                    "province_id":"2",
                    "activity_id":null,
                    "activity_rule_id":null,
                    "coupon_id":null,
                    "date_id":"2022-08-20",
                    "create_time":"2022-08-20 06:22:57",
                    "source_id":null,
                    "source_type_code":"2401",
                    "source_type_name":"????????????",
                    "sku_num":"2",
                    "split_original_amount":"80.0000",
                    "split_activity_amount":null,
                    "split_coupon_amount":null,
                    "split_total_amount":"80.0",
                    "ts":1660976577,
                    "row_op_ts":"2022-08-20 06:23:01.933Z"
                }
         */
                // Builder?????????????????????, ????????????
                return TradeSkuOrderBean.builder()
                    .skuId(value.getString("sku_id"))
                    .originalAmount(value.getBigDecimal("split_original_amount"))
                    .orderAmount(value.getBigDecimal("split_total_amount"))
                    .activityAmount(value.getBigDecimal("split_activity_amount") == null ? new BigDecimal(0) : value.getBigDecimal("split_activity_amount"))
                    .couponAmount(value.getBigDecimal("split_coupon_amount") == null ? new BigDecimal(0) : value.getBigDecimal("split_coupon_amount"))
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
                    
                    // ????????????????????????, ???????????????????????????
                    
                    if (maxTsDateState.value() == null) {
                        // ???????????????????????????, ?????????????????????, ??????5s?????????????????????
                        maxTsDateState.update(value);
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                    } else {
                        // ?????????????????????, ?????????????????????????????????
                        String last = maxTsDateState.value().getString("row_op_ts");
                        String current = value.getString("row_op_ts");
                        
                        // ??????current??????????????????last, ??????true?????????
                        if (AtguiguUtil.isLarger(current, last)) {
                            maxTsDateState.update(value);
                        }
                    }
                }
                
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<JSONObject> out) throws Exception {
                    // ???????????????????????????, ??????????????????
                    out.collect(maxTsDateState.value());
                    
                }
            });
    }
}
/*
????????????:
 ?????????????????????????????????
 
 1. ???????????????????????????????????????
    hadoop hbase redis zk kafka maxwell
    ?????????app ??????app  dimapp
    
    hbase??????:
        zk: deletall /hbase
        hdfs: /hbase
        
   kafka??????:
        zk:  /kafka
        
        kafka: $kafka_home/logs/* ??????
        
 2. ?????????redis
    ??????redis????????????????????????
        redis-server /etc/redis.conf
 
 3. ?????????6????????????????????????, ??????????????????
     maxwell ??????????????????????????????
     
 4. ??????
        id?????????
        
 5. ??????


???????????????io, ???????????????????????????????????????????????????

redis???phoenix????????????????????????

?????????(?????????)+????????????
    ??????????????????????????????



 */