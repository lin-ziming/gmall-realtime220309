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
        Table kwTable = tEnv.sqlQuery("select" +
                                          " kw, " +
                                          " et " +
                                          "from keyword_table " +
                                          "join lateral table(ik_analyzer(keyword)) on true");
        tEnv.createTemporaryView("kw_table", kwTable);
        // 4. 对分词后的次,开窗聚合统计次数
        Table result = tEnv.sqlQuery("select " +
                                         " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                                         " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                                         " 'search' source, " +
                                         " kw keyword,  " +
                                         " date_format(window_start, 'yyyy-MM-dd') cur_date, " +  // 这个是表示统计日期
                                         " count(*) keyword_count " +
                                         "from table( tumble( table kw_table, descriptor(et), interval '5' second ) ) " +
                                         "group by window_start, window_end, kw");
        
        // 5. 写出到 doris 中
        tEnv.executeSql("create table kw(" +
                            " stt string, " +
                            " edt string, " +
                            " source string, " +
                            " keyword string, " +
                            " cur_date string, " +
                            " keyword_count bigint " +
                            ")with(" +
                            "  'connector' = 'doris', " +
                            "  'fenodes' = 'hadoop162:7030', " +
                            "  'table.identifier' = 'gmall2022.dws_traffic_source_keyword_page_view_window', " +
                            "  'username' = 'root', " +
                            "  'password' = 'aaaaaa' " +
                            ")");
    
        result.executeInsert("kw");
        
        
    }
}
/*
 流量域来源关键词粒度页面浏览各窗口汇总表

搜索记录, 找到用户的搜索关键词, 统计关键词在每个窗口内出现的次数

数据源:
    页面日志

     "item" is not null
    "item_type" = "keyword",
    "last_page_id"= "search"

"华为手机"
小米手机
苹果手机
--------
对关键词进行分词: Ik分词器
"华为
手机"
小米
手机
苹果
手机

开窗聚合, 统计每个关键词的次数
  group window
  tvf
    选tvf
  over

把统计结果写出到oris

 */