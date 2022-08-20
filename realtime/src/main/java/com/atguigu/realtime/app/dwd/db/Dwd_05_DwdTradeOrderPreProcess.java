package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/20 8:33
 */
public class Dwd_05_DwdTradeOrderPreProcess extends BaseSqlApp {
    public static void main(String[] args) {
        new Dwd_05_DwdTradeOrderPreProcess().init(
            3005,
            2,
            "Dwd_05_DwdTradeOrderPreProcess"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 1. 读取ods_db
        readOdsDb(tEnv, "Dwd_05_DwdTradeOrderPreProcess");
        
        // 2. 读取字典表
        readBaseDic(tEnv);
        // 3. 过滤出order_detail
        Table orderDetail = tEnv.sqlQuery("select " +
                                        "data['id'] id, " +
                                        "data['order_id'] order_id, " +
                                        "data['sku_id'] sku_id, " +
                                        "data['sku_name'] sku_name, " +
                                        "data['create_time'] create_time, " +
                                        "data['source_id'] source_id, " +
                                        "data['source_type'] source_type, " +
                                        "data['sku_num'] sku_num, " +
                                        "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                                        "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount, " +
                                        "data['split_total_amount'] split_total_amount, " +
                                        "data['split_activity_amount'] split_activity_amount, " +
                                        "data['split_coupon_amount'] split_coupon_amount, " +
                                        "ts od_ts, " +
                                        "pt " +
                                        "from ods_db " +
                                        "where `database`='gmall2022' " +
                                        "and `table`='order_detail' " +
                                        "and `type`='insert'");
        
        tEnv.createTemporaryView("order_detail", orderDetail);
        
        orderDetail.execute().print();
    
        // 4. 过滤出order_info
        
        // 5. 过滤 活动
        
        // 6. 过滤出优惠券
        
        // 7. 5张表的join
        
        // 8. 定义动态与输出的topic关联
        
        // 9. 把join的结果写出到输出的表
    }
}
/*
order_detail
    join
        ttl设置多少合理?
            下单会产出一条order_info和n条order_detail, 几乎同时产生   ttl 10s
            
            订单取消. order_info一条数据发送update, order_detail不会发生任何
                取消的时候可能已经过去30m分钟, 也需要去join 以前的详情  ttl 1h
                
order_info
    left join
activity
    left join
coupon
    lookup join
dic


---
写kafka要用 upsert-kafka


*/