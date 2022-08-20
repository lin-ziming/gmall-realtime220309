package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/20 11:16
 */
public class Dwd_08_DwdTradePayDetailSuc extends BaseSqlApp {
    public static void main(String[] args) {
        new Dwd_08_DwdTradePayDetailSuc().init();
    }
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        
    }
}

/*
1. payment info

2. 需要详情信息和订单信息
    dwd_trade_order_detail
    
3. 字典表
 */