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
        
        
        // 1. 按照 order_detail_id 去重
        SingleOutputStreamOperator<JSONObject> distinctedStream = distinctByOrderDetailId(stream);
        // 2. 把数据封装到pojo中
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(distinctedStream);
        // 3. 按照 sku_id 分组 开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDim = windowAndAgg(beanStream);
        // 4. 补充维度信息
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithDim = addDim(beanStreamWithoutDim);
        // 5. 写出到doris中
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
                    "sku_name":"十月稻田 辽河长粒香 东北大米 5kg",
                    "province_id":"2",
                    "activity_id":null,
                    "activity_rule_id":null,
                    "coupon_id":null,
                    "date_id":"2022-08-20",
                    "create_time":"2022-08-20 06:22:57",
                    "source_id":null,
                    "source_type_code":"2401",
                    "source_type_name":"用户查询",
                    "sku_num":"2",
                    "split_original_amount":"80.0000",
                    "split_activity_amount":null,
                    "split_coupon_amount":null,
                    "split_total_amount":"80.0",
                    "ts":1660976577,
                    "row_op_ts":"2022-08-20 06:23:01.933Z"
                }
         */
                // Builder模式来构造对象, 比较方便
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
/*
异步超时:
 其他原因导致的异步超时
 
 1. 用到的所有集群全部正常开启
    hadoop hbase redis zk kafka maxwell
    预处理app 详情app  dimapp
    
    hbase修复:
        zk: deletall /hbase
        hdfs: /hbase
        
   kafka修复:
        zk:  /kafka
        
        kafka: $kafka_home/logs/* 删光
        
 2. 检查下redis
    启动redis一定要正确的配置
        redis-server /etc/redis.conf
 
 3. 检查下6张维度表是否齐全, 并且都有数据
     maxwell 同步下所有的维度数据
     
 4. 调试
        id传错了
        
 5. 找我


要使用异步io, 用的数据库客户端必须提供异步客户端

redis和phoenix都没有异步客户端

多线程(线程池)+多客户端
    每个线程搞一个客户端



 */