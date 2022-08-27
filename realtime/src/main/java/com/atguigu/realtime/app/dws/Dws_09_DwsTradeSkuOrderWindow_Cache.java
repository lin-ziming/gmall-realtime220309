package com.atguigu.realtime.app.dws;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TradeSkuOrderBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.DimUtil;
import com.atguigu.realtime.util.DruidDSUtil;
import com.atguigu.realtime.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/8/26 13:36
 */
public class Dws_09_DwsTradeSkuOrderWindow_Cache extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow_Cache().init(
            4009,
            2,
            "Dws_09_DwsTradeSkuOrderWindow",
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
        addDim(beanStreamWithoutDim).print();
        
        // 5. 写出到doris中
        
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> addDim(
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDim) {
        
        // 补充维度信息:条数据, 需要查询6张维度表
        return beanStreamWithoutDim.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
    
            private Jedis redisClient;
            private DruidDataSource druidDataSource;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                druidDataSource = DruidDSUtil.getDruidDataSource();  // 连接池
                redisClient = RedisUtil.getRedisClient();
    
            }
    
            @Override
            public void close() throws Exception {
                druidDataSource.close();
                redisClient.close(); // 归还redis连接
            }
    
            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean bean) throws Exception {
                DruidPooledConnection phoenixConn = druidDataSource.getConnection();
                // 1. 查找dim_sku_info
                // select * from dim_sku_info where id='1';
                // JSONObject : {"列名": 值, ...}
                JSONObject skuInfo = DimUtil.readDim(redisClient, phoenixConn, "dim_sku_info", bean.getSkuId());
                bean.setSkuName(skuInfo.getString("SKU_NAME"));
                bean.setTrademarkId(skuInfo.getString("TM_ID"));
                bean.setSpuId(skuInfo.getString("SPU_ID"));
                bean.setCategory3Id(skuInfo.getString("CATEGORY3_ID"));
                
                //2. tm
                JSONObject baseTrademark = DimUtil.readDim(redisClient,phoenixConn, "dim_base_trademark", bean.getTrademarkId());
                bean.setTrademarkName(baseTrademark.getString("TM_NAME"));
                
                // 3. spu
                JSONObject spuInfo = DimUtil.readDim(redisClient,phoenixConn, "dim_spu_info", bean.getSpuId());
                bean.setSpuName(spuInfo.getString("SPU_NAME"));
                
                // 4. c3
                JSONObject c3 = DimUtil.readDim(redisClient,phoenixConn, "dim_base_category3", bean.getCategory3Id());
                bean.setCategory3Name(c3.getString("NAME"));
                bean.setCategory2Id(c3.getString("CATEGORY2_ID"));
                
                // 5. c2
                JSONObject c2 = DimUtil.readDim(redisClient,phoenixConn, "dim_base_category2", bean.getCategory2Id());
                bean.setCategory2Name(c2.getString("NAME"));
                bean.setCategory1Id(c2.getString("CATEGORY1_ID"));
                
                // 6. c1
                JSONObject c1 = DimUtil.readDim(redisClient,phoenixConn, "dim_base_category1", bean.getCategory1Id());
                bean.setCategory1Name(c1.getString("NAME"));
                
                
                phoenixConn.close();  // 归还给连接池
                return bean;
            }
        });
        
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
                        
                        bean.setCurDate(AtguiguUtil.toDate(System.currentTimeMillis()));
                        
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
维度补充:
    连接6次, 查询6次数据库
    
    sku_id  10
    
 缓存优化:
    第一次查从数据库, 以后从缓存
    
    内存
    
    1. 堆内存(flink的状态)
        好处: 本地内存, 读写不需要经过网络, 速度极快
        
        坏处:
            1. 占用flink的计算内存
            2. 维度会变化, 堆内存中的维度没有办法及时更新
    
  
    2. 外置内存(redis) 旁路缓存
        坏处:
            1. 每次都需要通过网络
            
        好处:
            维度会变化, 堆内存中的维度没有办法及时更新
    


交易域SKU粒度下单各窗口

数据源:
    下单事务事实 (dwd的下单明细)

实现思路:
    按照sku_id分组, 开窗, 聚合各个指标
       总的销售额

    结果写出到doris中

1. 去重问题
    下单详情表的来源: 预处理表
        做了个过滤

    预处理表是如何实现?
            order_detail
                join
            order_info
               left join
            详情活动
                left join
            详情优惠券
                look up join
            字典表
    因为有left join, 对消费者来说, 同一个详情会可能产生重复数据

    所以现在我们是sku 粒度, 必须去重


    去重思路:
        数据最全的那条记录

        1.  用会话窗口
            row_op_ts 他时间最大的那个
            把同一个详情id的分组, 开窗, 取时间最大的那个

            什么窗口?
                把最多3条数据放入到同一个窗口内
                这个最多三条数据, 从时间上来说,差不不会太大

                当下单的时候, 详情和活动, 优惠券几乎同时生成, 由于网络原因导致的差别, 5s足够

                会话窗口, gap=5s

                等到窗口触发计算的时候, 找到时间最大的那个

        2. 定时器
            同一个详情id, 第一条数据进来的时候注册一个 5s 后触发的定时器
            每来一条数据, 把数据存入到状态中, 后面来的数据和状态中的做比较, 时间变大, 则更新状态

            等到定时器触发的时候, 则最大的比较出来了

            会话窗口和定时器哪个更优?

               定时器更优: 时效性更好

        3. 如果在统计聚合计算的时候, 根本就用不到右表的数据, 就不需要等最全的了
            用第一条数据

2. 如何补充其他维度
    维度在什么地方? hbase中(Phoenix中)

    根据id查找对应的表得到维度

    执行: select ....  from t where id=?
    每来一条数据, 就需要去hbase中查找对应的维度: 6张维度表
        sku_info base_trademark spu_info 3级 2级 1级品类


    聚合前查找维度还是聚合后查找维度?
        聚合后, 这样效率更高





`stt`                          DATETIME comment '窗口起始时间',
`edt`                          DATETIME comment '窗口结束时间',
`trademark_id`                 VARCHAR(10) comment '品牌ID',
`trademark_name`               VARCHAR(128) comment '品牌名称',
`category1_id`                 VARCHAR(10) comment '一级品类ID',
`category1_name`               VARCHAR(128) comment '一级品类名称',
`category2_id`                 VARCHAR(10) comment '二级品类ID',
`category2_name`               VARCHAR(128) comment '二级品类名称',
`category3_id`                 VARCHAR(10) comment '三级品类ID',
`category3_name`               VARCHAR(128) comment '三级品类名称',
`sku_id`                       VARCHAR(10) comment 'SKU_ID',
`sku_name`                     VARCHAR(128) comment 'SKU 名称',
`spu_id`                       VARCHAR(10) comment 'SPU_ID',
`spu_name`                     VARCHAR(128) comment 'SPU 名称',
`cur_date`                     DATE comment '当天日期',
`order_origin_total_amount`    DECIMAL(16, 2) replace comment '订单原始金额',
`order_activity_reduce_amount` DECIMAL(16, 2) replace comment '订单活动减免金额',
`order_coupon_reduce_amount`   DECIMAL(16, 2) replace comment '订单优惠券减免金额',
`order_amount`                 DECIMAL(16, 2) replace comment '订单总额' = 订单原始金额 - 订单活动减免金额 - 订单优惠券减免金额




 */