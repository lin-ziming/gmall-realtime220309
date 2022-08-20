package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/20 14:10
 */
public class Dwd_06_DwdTradeOrderDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new Dwd_06_DwdTradeOrderDetail().init(
            3006,
            2,
            "Dwd_06_DwdTradeOrderDetail"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取预处理表
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
                            "row_op_ts timestamp_ltz(3) " +
                            ")" + SQLUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_ORDER_PRE_PROCESS, "Dwd_06_DwdTradeOrderDetail"));
        
        // 2. 过滤出下单详情
        Table result = tEnv.sqlQuery("select " +
                                         "id, " +
                                         "order_id, " +
                                         "user_id, " +
                                         "sku_id, " +
                                         "sku_name, " +
                                         "province_id, " +
                                         "activity_id, " +
                                         "activity_rule_id, " +
                                         "coupon_id, " +
                                         "date_id, " +
                                         "create_time, " +
                                         "source_id, " +
                                         "source_type source_type_code, " +
                                         "source_type_name, " +
                                         "sku_num, " +
                                         "split_original_amount, " +
                                         "split_activity_amount, " +
                                         "split_coupon_amount, " +
                                         "split_total_amount, " +
                                         "od_ts ts, " +
                                         "row_op_ts " +
                                         "from dwd_trade_order_pre_process " +
                                         "where `type`='insert'");
        
        // 3. 写出到kafka
        tEnv.executeSql("create table dwd_trade_order_detail( " +
                            "id string, " +
                            "order_id string, " +
                            "user_id string, " +
                            "sku_id string, " +
                            "sku_name string, " +
                            "province_id string, " +
                            "activity_id string, " +
                            "activity_rule_id string, " +
                            "coupon_id string, " +
                            "date_id string, " +
                            "create_time string, " +
                            "source_id string, " +
                            "source_type_code string, " +
                            "source_type_name string, " +
                            "sku_num string, " +
                            "split_original_amount string, " +
                            "split_activity_amount string, " +
                            "split_coupon_amount string, " +
                            "split_total_amount string, " +
                            "ts bigint, " +
                            "row_op_ts timestamp_ltz(3) " +
                            ")" + SQLUtil.getKafkaSink(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        
        result.executeInsert("dwd_trade_order_detail");
        
    }
}
