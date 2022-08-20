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
 * @Date 2022/8/20 11:16
 */
public class Dwd_08_DwdTradePayDetailSuc extends BaseSqlApp {
    public static void main(String[] args) {
        new Dwd_08_DwdTradePayDetailSuc().init(
            3008,
            2,
            "Dwd_08_DwdTradePayDetailSuc"
        );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(30));
        
        // 1. 下单事务事实表
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
                            ")" + SQLUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, "Dwd_08_DwdTradePayDetailSuc"));
        
        // 2. 支付表
        readOdsDb(tEnv, "Dwd_08_DwdTradePayDetailSuc");
        Table paymentInfo = tEnv.sqlQuery("select " +
                                        "data['user_id'] user_id, " +
                                        "data['order_id'] order_id, " +
                                        "data['payment_type'] payment_type, " +
                                        "data['callback_time'] callback_time, " +
                                        "`pt`, " +
                                        "ts " +
                                        "from ods_db " +
                                        "where `database`='gmall2022' " +
                                        "and `table`='payment_info' " +
                                        "and `type`='update' " +
                                        "and `old`['payment_status'] is not null " +
                                        "and `data`['payment_status']='1602'");
        tEnv.createTemporaryView("payment_info", paymentInfo);
        // 3. 字典表
        readBaseDic(tEnv);
        // 4. 3张表join
        Table result = tEnv.sqlQuery("select " +
                                        "od.id order_detail_id, " +
                                        "od.order_id, " +
                                        "od.user_id, " +
                                        "od.sku_id, " +
                                        "od.sku_name, " +
                                        "od.province_id, " +
                                        "od.activity_id, " +
                                        "od.activity_rule_id, " +
                                        "od.coupon_id, " +
                                        "pi.payment_type payment_type_code, " +
                                        "dic.dic_name payment_type_name, " +
                                        "pi.callback_time, " +
                                        "od.source_id, " +
                                        "od.source_type_code, " +
                                        "od.source_type_name, " +
                                        "od.sku_num, " +
                                        "od.split_original_amount, " +
                                        "od.split_activity_amount, " +
                                        "od.split_coupon_amount, " +
                                        "od.split_total_amount split_payment_amount, " +
                                        "pi.ts, " +
                                        "od.row_op_ts row_op_ts " +
                                        "from payment_info pi " +
                                        "join dwd_trade_order_detail od on pi.order_id=od.order_id " +
                                        "join base_dic for system_time as of pi.pt as dic on pi.payment_type=dic.dic_code ");
        // 5. 写出去
        tEnv.executeSql("create table dwd_trade_pay_detail_suc( " +
                            "order_detail_id string, " +
                            "order_id string, " +
                            "user_id string, " +
                            "sku_id string, " +
                            "sku_name string, " +
                            "province_id string, " +
                            "activity_id string, " +
                            "activity_rule_id string, " +
                            "coupon_id string, " +
                            "payment_type_code string, " +
                            "payment_type_name string, " +
                            "callback_time string, " +
                            "source_id string, " +
                            "source_type_code string, " +
                            "source_type_name string, " +
                            "sku_num string, " +
                            "split_original_amount string, " +
                            "split_activity_amount string, " +
                            "split_coupon_amount string, " +
                            "split_payment_amount string, " +
                            "ts bigint, " +
                            "row_op_ts timestamp_ltz(3) " +
                            ")" + SQLUtil.getKafkaSink(Constant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC));
        
        
        result.executeInsert("dwd_trade_pay_detail_suc");
//        tEnv.executeSql("insert into dwd_trade_pay_detail_suc select * form " + result);
        
    }
}

/*
1. payment info

2. 需要详情信息和订单信息
    dwd_trade_order_detail
    
3. 字典表
 */