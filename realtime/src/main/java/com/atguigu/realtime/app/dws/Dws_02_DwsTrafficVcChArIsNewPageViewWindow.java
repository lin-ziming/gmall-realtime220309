package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.TrafficPageViewBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

import static com.atguigu.realtime.common.Constant.*;

/**
 * @Author lzc
 * @Date 2022/8/25 9:09
 */
public class Dws_02_DwsTrafficVcChArIsNewPageViewWindow extends BaseAppV2 {
    
    
    public static void main(String[] args) {
        new Dws_02_DwsTrafficVcChArIsNewPageViewWindow().init(
            4002,
            2,
            "Dws_02_DwsTrafficVcChArIsNewPageViewWindow",
            TOPIC_DWD_TRAFFIC_PAGE, TOPIC_DWD_TRAFFIC_UV_DETAIL, TOPIC_DWD_TRAFFIC_UJ_DETAIL
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          HashMap<String, DataStreamSource<String>> streams) {
        streams.get(TOPIC_DWD_TRAFFIC_UJ_DETAIL).print(TOPIC_DWD_TRAFFIC_UJ_DETAIL);
        
        // 1. 把里转成同一种类型, 然后union成一个流
        DataStream<TrafficPageViewBean> beanStream = parseAndUnionOne(streams);
        beanStream.print();
    
        // 2. 开窗聚合
        
        // 3. 写出到doris中
        
        
    }
    
    private DataStream<TrafficPageViewBean> parseAndUnionOne(HashMap<String, DataStreamSource<String>> streams) {
        
        // 1. pv sv during_sum
        SingleOutputStreamOperator<TrafficPageViewBean> pvSvDuringSumStream = streams
            .get(TOPIC_DWD_TRAFFIC_PAGE)
            .map(json -> {
                // 计算 pv sv dursum
                JSONObject obj = JSON.parseObject(json);
            
                JSONObject common = obj.getJSONObject("common");
                JSONObject page = obj.getJSONObject("page");
            
                String vc = common.getString("vc");
                String ch = common.getString("ch");
                String ar = common.getString("ar");
                String isNew = common.getString("is_new");
            
                Long uvCt = 0L;
                Long svCt = page.getString("last_page_id") == null ? 1L : 0L;
                Long pvCt = 1L;
                Long durSum = page.getLong("during_time");
                Long ujCt = 0L;
            
                Long ts = obj.getLong("ts");
            
            
                return new TrafficPageViewBean("", "", // stt edt 只能开窗聚合后才有窗口时间
                                               vc, ch, ar, isNew,
                                               "",  // 当天日期   等待聚合后, 再添加也不迟
                                               uvCt, svCt, pvCt, durSum, ujCt,  // 指标列
                                               ts // 时间戳
                );
            
            });
        
        // 2. uv
        SingleOutputStreamOperator<TrafficPageViewBean> uvStream = streams
            .get(TOPIC_DWD_TRAFFIC_UV_DETAIL)
            .map(json -> {
                // 计算 pv sv dursum
                JSONObject obj = JSON.parseObject(json);
            
                JSONObject common = obj.getJSONObject("common");
                JSONObject page = obj.getJSONObject("page");
            
                String vc = common.getString("vc");
                String ch = common.getString("ch");
                String ar = common.getString("ar");
                String isNew = common.getString("is_new");
            
                Long uvCt = 1L;
                Long svCt = 0L;
                Long pvCt = 0L;
                Long durSum = 0L;
                Long ujCt = 0L;
            
                Long ts = obj.getLong("ts");
            
            
                return new TrafficPageViewBean("", "", // stt edt 只能开窗聚合后才有窗口时间
                                               vc, ch, ar, isNew,
                                               "",  // 当天日期   等待聚合后, 再添加也不迟
                                               uvCt, svCt, pvCt, durSum, ujCt,  // 指标列
                                               ts // 时间戳
                );
            
            });
    
        // 2. uj
        SingleOutputStreamOperator<TrafficPageViewBean> ujStream = streams
            .get(TOPIC_DWD_TRAFFIC_UJ_DETAIL)
            .map(json -> {
                // 计算 pv sv dursum
                JSONObject obj = JSON.parseObject(json);
            
                JSONObject common = obj.getJSONObject("common");
                JSONObject page = obj.getJSONObject("page");
            
                String vc = common.getString("vc");
                String ch = common.getString("ch");
                String ar = common.getString("ar");
                String isNew = common.getString("is_new");
            
                Long uvCt = 0L;
                Long svCt = 0L;
                Long pvCt = 0L;
                Long durSum = 0L;
                Long ujCt = 1L;
            
                Long ts = obj.getLong("ts");
            
            
                return new TrafficPageViewBean("", "", // stt edt 只能开窗聚合后才有窗口时间
                                               vc, ch, ar, isNew,
                                               "",  // 当天日期   等待聚合后, 再添加也不迟
                                               uvCt, svCt, pvCt, durSum, ujCt,  // 指标列
                                               ts // 时间戳
                );
            
            });
        
        return pvSvDuringSumStream.union(uvStream, ujStream);
    
    }
}
/*
版本-渠道-地区-访客类别 粒度下:

会话数
    数据源:
        页面日志
            过滤出last_page_id is null的数据
页面浏览数 pv
    数据源:
        页面日志
            直接统计个数
浏览总时长
    数据源:
        页面日志
            直接sum(during_time)
独立访客数 uv
    数据源:
        uv详情
            直接统计个数
跳出会话数
    数据源:
        跳出明细
            直接统计个数

总结:
    5个指标, 来源于3个流

-------
pv
版本1-华为渠道-深圳-新用户  1
uv
版本1-华为渠道-深圳-新用户  1
uj
版本1-华为渠道-深圳-新用户  1

pv
版本1-华为渠道-深圳-新用户  1  0 0
uv
版本1-华为渠道-深圳-新用户  0 1  0
uj
版本1-华为渠道-深圳-新用户  0 0 1

3个流做成1个流: connect union join
选union的原因: 一次多个流union到一起.
但是union的类型必须一致
=>

版本1-华为渠道-深圳-新用户  1  0  0
版本1-华为渠道-深圳-新用户  0  1  0
版本1-华为渠道-深圳-新用户  0  0  1
...

开窗聚合 =>
-----------------------
窗口   维度                             pv    uv   uj
0-5    版本1-华为渠道-深圳-新用户        200   10   20
...


写出doris中
 */
