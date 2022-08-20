package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

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
        tEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(30));
//        tEnv.getConfig().getConfiguration().setString("table.exec.state.ttl", "30 minute");
        
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
        // 4. 过滤出order_info
        Table orderInfo = tEnv.sqlQuery("select " +
                                            "data['id'] id, " +
                                            "data['user_id'] user_id, " +
                                            "data['province_id'] province_id, " +
                                            "data['operate_time'] operate_time, " +
                                            "data['order_status'] order_status, " +
                                            "`type`, " +
                                            "`old`, " +
                                            "ts oi_ts " +
                                            "from ods_db " +
                                            "where `database`='gmall2022' " +
                                            "and `table`='order_info' " +
                                            "and (`type`='insert'" +
                                            "     or `type`='update'" +
                                            ")");
        
        tEnv.createTemporaryView("order_info", orderInfo);
        // 5. 过滤 活动
        Table orderDetailActivity = tEnv.sqlQuery("select " +
                                                      "data['order_detail_id'] order_detail_id, " +
                                                      "data['activity_id'] activity_id, " +
                                                      "data['activity_rule_id'] activity_rule_id " +
                                                      "from ods_db " +
                                                      "where `database`='gmall2022' " +
                                                      "and `table`='order_detail_activity' " +
                                                      "and `type`='insert'");
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
        // 6. 过滤出优惠券
        Table orderDetailCoupon = tEnv.sqlQuery("select " +
                                                    "data['order_detail_id'] order_detail_id, " +
                                                    "data['coupon_id'] coupon_id " +
                                                    "from ods_db " +
                                                    "where `database`='gmall2022' " +
                                                    "and `table`='order_detail_coupon' " +
                                                    "and `type`='insert'");
        
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
        // 7. 5张表的join
        Table result = tEnv.sqlQuery("select " +
                                         "od.id, " +
                                         "od.order_id, " +
                                         "oi.user_id, " +
                                         "oi.order_status, " +
                                         "od.sku_id, " +
                                         "od.sku_name, " +
                                         "oi.province_id, " +
                                         "act.activity_id, " +
                                         "act.activity_rule_id, " +
                                         "cou.coupon_id, " +
                                         "date_format(od.create_time, 'yyyy-MM-dd') date_id, " +
                                         "od.create_time, " +
                                         "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id, " +
                                         "oi.operate_time, " +
                                         "od.source_id, " +
                                         "od.source_type, " +
                                         "dic.dic_name source_type_name, " +
                                         "od.sku_num, " +
                                         "od.split_original_amount, " +
                                         "od.split_activity_amount, " +
                                         "od.split_coupon_amount, " +
                                         "od.split_total_amount, " +
                                         "oi.`type`, " +
                                         "oi.`old`, " +
                                         "od.od_ts, " +
                                         "oi.oi_ts, " +
                                         "current_row_timestamp() row_op_ts " + // 这个每条数据计算的的时候的系统时间
                                         "from order_detail od " +
                                         "join order_info oi on od.order_id=oi.id " +
                                         "join base_dic for system_time as of od.pt as dic on od.source_type=dic.dic_code " +
                                         "left join order_detail_activity act on od.id=act.order_detail_id " +
                                         "left join order_detail_coupon cou on od.id=cou.order_detail_id ");
        
        
        // 8. 定义动态与输出的topic关联
        tEnv.executeSql("create table dwd_trade_order_pre_process( " +
                            "id string, " +
                            "order_id string, " +
                            "user_id string, " +
                            "order_status string, " +
                            "sku_id string, " +
                            "sku_name string, " +
                            "province_id string, " +
                            "activity_id string, " +
                            "activity_rule_id string, " +
                            "coupon_id string, " +
                            "date_id string, " +
                            "create_time string, " +
                            "operate_date_id string, " +
                            "operate_time string, " +
                            "source_id string, " +
                            "source_type string, " +
                            "source_type_name string, " +
                            "sku_num string, " +
                            "split_original_amount string, " +
                            "split_activity_amount string, " +
                            "split_coupon_amount string, " +
                            "split_total_amount string, " +
                            "`type` string, " +
                            "`old` map<string,string>, " +
                            "od_ts bigint, " +
                            "oi_ts bigint, " +
                            "row_op_ts timestamp_ltz(3), " +
                            "primary key(id) not enforced " +
                            ")" + SQLUtil.getUpsertKafkaSink(Constant.TOPIC_DWD_TRADE_ORDER_PRE_PROCESS)
        );
        
        // 9. 把join的结果写出到输出的表
        result.executeInsert("dwd_trade_order_pre_process");
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



1  100.1        null      生成时间
1  100.1         10       生成时间

由于有left join的存在, 将来消费的时候会有重复数据, 需要去重
保留数据最全, 就是数据生成时间最大的



*/