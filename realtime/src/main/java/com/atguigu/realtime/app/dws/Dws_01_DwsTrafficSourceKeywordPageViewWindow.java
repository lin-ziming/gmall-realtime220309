package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.function.IkAnalyzer;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/24 14:15
 */
public class Dws_01_DwsTrafficSourceKeywordPageViewWindow extends BaseSqlApp {
    public static void main(String[] args) {
        new Dws_01_DwsTrafficSourceKeywordPageViewWindow().init(
            4001,
            2,
            "Dws_01_DwsTrafficSourceKeywordPageViewWindow"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 1. 创建动态表与topic关联: page
        tEnv.executeSql("create table page(" +
                            " page map<string, string>, " +
                            " ts bigint, " +
                            " et as to_timestamp_ltz(ts, 3), " +
                            " watermark for et as et - interval '3' second " +
                            ")"
                            + SQLUtil.getKafkaSource(Constant.TOPIC_DWD_TRAFFIC_PAGE, "Dws_01_DwsTrafficSourceKeywordPageViewWindow"));
        // 2. 过滤出关键词
        Table keywordTable = tEnv.sqlQuery("select " +
                                        " page['item'] keyword, " +
                                        " et " +
                                        "from page " +
                                        "where page['last_page_id']= 'search' " +
                                        "and page['item_type']='keyword' " +
                                        "and page['item'] is not null");
        tEnv.createTemporaryView("keyword_table", keywordTable);
    
        // 3. 对关键词进行分词
        // 自定义函数  标量 制表 聚合 制表聚合
        tEnv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);
        tEnv.sqlQuery("select" +
                          " kw, " +
                          " et " +
                          "from keyword_table " +
                          "join lateral table(ik_analyzer(keyword)) on true").execute().print();
        
        
        // 4. 对分词后的次,开窗聚合统计次数
        
        // 5. 写出到 doris 中
        
    }
}
/*

 */