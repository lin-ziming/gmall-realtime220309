package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/19 14:32
 */
public class Dwd_04_DwdTradeCartAdd extends BaseSqlApp {
    public static void main(String[] args) {
        new Dwd_04_DwdTradeCartAdd().init(3004, 2, "Dwd_04_DwdTradeCartAdd");
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 1. 读取ods_db数据
        readOdsDb(tEnv, "Dwd_04_DwdTradeCartAdd");
        
        // 2. 过滤cart_info数据
        Table cartInfo = tEnv.sqlQuery("select " +
                                           "data['id'] id, " +
                                           "data['user_id'] user_id, " +
                                           "data['sku_id'] sku_id, " +
                                           "data['source_id'] source_id, " +
                                           "data['source_type'] source_type, " +
                                           "if(`type` = 'insert', " +
                                           "data['sku_num'],cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num, " +
                                           "ts, " +
                                           "pt " +
                                           "from ods_db " +
                                           "where `database`='gmall2022' " +
                                           "and `table`='cart_info' " +
                                           "and (" +
                                           " `type`='insert' " +
                                           "  or (`type`='update'" +
                                           "      and `old`['sku_num'] is not null " + // 由于sku_num变化导致的update
                                           "      and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)" +
                                           "     )" +
                                           ")");
        tEnv.createTemporaryView("cart_info", cartInfo);
        
        // 3. 读取字典表
        readBaseDic(tEnv);
        // 4. 维度退化
        Table result = tEnv.sqlQuery("select " +
                                         "ci.id, " +
                                         "ci.user_id, " +
                                         "ci.sku_id, " +
                                         "ci.source_id, " +
                                         "ci.source_type, " +
                                         "dic.dic_name source_type_name, " +
                                         "ci.sku_num, " +
                                         "ci.ts " +
                                         "from cart_info ci " +
                                         "join base_dic for system_time as of ci.pt as dic " +
                                         "on ci.source_type=dic.dic_code");
        
        // 5. 定义一个动态表与kafka的topic关联
        tEnv.executeSql("create table dwd_trade_cart_add( " +
                          "id string, " +
                          "user_id string, " +
                          "sku_id string, " +
                          "source_id string, " +
                          "source_type_code string, " +
                          "source_type_name string, " +
                          "sku_num string, " +
                          "ts bigint " +
                          ")" + SQLUtil.getKafkaSink(Constant.TOPIC_DWD_TRADE_CART_ADD)
        );
    
        result.executeInsert("dwd_trade_cart_add");
        
        
    }
}
